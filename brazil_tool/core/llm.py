# -*- coding: utf-8 -*-
import json
import logging
import re
import urllib.request
import urllib.error
from typing import Optional, Dict
from brazil_tool.core.models import Invoice
from brazil_tool.core.utils import br_to_float
from brazil_tool.core.pdf import get_first_page_image_data_url

def _trim_llm_text(text: str, max_chars: int) -> str:
    if not text: return ""
    text = text.strip()
    if len(text) <= max_chars: return text
    return text[:max_chars] + "\n[TRUNCATED]"

def _extract_json_from_text(content: str) -> Optional[dict]:
    if not content: return None
    cleaned = content.strip()
    cleaned = re.sub(r"```(?:json)?", "", cleaned, flags=re.I)
    cleaned = cleaned.replace("```", "").strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start: return None
    blob = cleaned[start:end + 1]
    try:
        return json.loads(blob)
    except Exception:
        return None

def _call_llm_chat(messages: list, config: dict) -> Optional[dict]:
    endpoint = (config.get("llm_endpoint") or "").strip()
    if not endpoint: return None
    if endpoint.endswith("/"): endpoint = endpoint[:-1]
    if not endpoint.endswith("/chat/completions"): endpoint = endpoint + "/chat/completions"

    payload = {
        "model": config.get("llm_model") or "local-model",
        "messages": messages,
        "temperature": float(config.get("llm_temperature", 0.1)),
        "max_tokens": int(config.get("llm_max_tokens", 600))
    }

    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    api_key = (config.get("llm_api_key") or "").strip()
    if api_key: headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(endpoint, data=data, headers=headers, method="POST")
    timeout = int(config.get("llm_timeout_sec", 30))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8", errors="ignore"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"LLM HTTP {e.code}: {err_body[:500]}")
    except Exception as e:
        raise RuntimeError(f"LLM Request Failed: {e}")

def _parse_llm_response(resp: dict) -> Optional[dict]:
    import re # Local import for strictness if needed, but module level is fine
    if not isinstance(resp, dict): return None
    content = ""
    if "choices" in resp and resp["choices"]:
        choice = resp["choices"][0]
        message = choice.get("message") or {}
        content = message.get("content") or choice.get("text") or ""
    elif "output_text" in resp:
        content = resp.get("output_text") or ""
    elif "content" in resp and isinstance(resp.get("content"), str):
        content = resp.get("content")

    return _extract_json_from_text(content)

def apply_llm_result(invoice: Invoice, llm_data: dict) -> bool:
    """Apply LLM results to invoice if fields are missing."""
    if not isinstance(llm_data, dict): return False
    changed = False

    def set_if_missing(attr: str, value, transform=None):
        nonlocal changed
        if value is None: return
        if isinstance(value, str) and not value.strip(): return
        current = getattr(invoice, attr)
        if current is None or (isinstance(current, str) and not current.strip()):
            new_val = transform(value) if transform else value
            setattr(invoice, attr, new_val)
            changed = True

    set_if_missing("numero", llm_data.get("invoice_number") or llm_data.get("numero"))
    set_if_missing("serie", llm_data.get("serie"))
    set_if_missing("natureza_operacao", llm_data.get("natureza_operacao"))
    set_if_missing("data_emissao", llm_data.get("issue_date") or llm_data.get("data_emissao"))
    set_if_missing("total_nota", llm_data.get("total_amount"), transform=lambda v: br_to_float(str(v)))
    set_if_missing("emitente_nome", llm_data.get("issuer_name") or llm_data.get("emitente_nome"))
    set_if_missing("emitente_cnpj", llm_data.get("issuer_cnpj") or llm_data.get("emitente_cnpj"))
    set_if_missing("destinatario_nome", llm_data.get("recipient_name") or llm_data.get("destinatario_nome"))
    set_if_missing("destinatario_cnpj", llm_data.get("recipient_cnpj") or llm_data.get("destinatario_cnpj"))
    set_if_missing("llm_table_note", llm_data.get("table_note") or llm_data.get("aux_note"))

    return changed

def run_llm_assist(file_path: str, text: str, config: dict) -> Optional[dict]:
    """Orchestrate LLM assistance."""
    if not config.get("enable_llm", False): return None

    max_chars = int(config.get("llm_max_chars", 8000))
    text_payload = _trim_llm_text(text, max_chars)
    if not text_payload: return None

    image_url = None
    if config.get("llm_use_multimodal", True):
        image_url = get_first_page_image_data_url(file_path, config.get("poppler_path"))

    system_prompt = (
        "你是发票字段抽取助手。请根据提供的发票文本/图片抽取字段，"
        "仅输出 JSON，不要任何额外说明或 Markdown。"
    )
    user_prompt = (
        "请返回如下 JSON 字段（未知填 null 或空字符串）：\n"
        "{\n"
        "  \"invoice_number\": \"发票号\",\n"
        "  \"serie\": \"序列\",\n"
        "  \"natureza_operacao\": \"操作性质\",\n"
        "  \"issue_date\": \"开票日期\",\n"
        "  \"total_amount\": \"总金额\",\n"
        "  \"issuer_name\": \"开票方名称\",\n"
        "  \"issuer_cnpj\": \"开票方CNPJ\",\n"
        "  \"recipient_name\": \"收货方名称\",\n"
        "  \"recipient_cnpj\": \"收货方CNPJ\",\n"
        "  \"table_note\": \"辅助表说明（简短中文摘要，1-2句话）\"\n"
        "}\n\n"
        "发票文本如下：\n"
        f"{text_payload}"
    )

    if image_url:
        # STRATEGY: Prioritize Text-Only (UI Code Elements) -> Fallback to Multimodal
        # 1. Try Text-Only first
        messages_text_only = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        try:
            resp_text = _call_llm_chat(messages_text_only, config)
            parsed_text = _parse_llm_response(resp_text)
        except Exception as e:
            logging.warning("LLM text-only request failed: %s", e)
            parsed_text = None
        
        # 2. Validate Result
        is_valid = False
        if parsed_text:
            # Simple validation: Check for key fields
            # If we have at least an amount or an invoice number, it's likely a success.
            # Adjust strictness as needed.
            has_amount = bool(parsed_text.get("total_amount") or parsed_text.get("total_nota"))
            has_number = bool(parsed_text.get("invoice_number") or parsed_text.get("numero"))
            has_cnpj = bool(parsed_text.get("issuer_cnpj") or parsed_text.get("emitente_cnpj"))
            
            # We consider it valid if we found (Amount AND (Number OR CNPJ))
            if has_amount and (has_number or has_cnpj):
                is_valid = True

        if is_valid:
            return parsed_text
        
        # 3. If Text-Only failed, combine Text + Image (Multimodal)
        messages_multimodal = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": [
                {"type": "text", "text": user_prompt},
                {"type": "image_url", "image_url": {"url": image_url}}
            ]}
        ]
        try:
            resp_multimodal = _call_llm_chat(messages_multimodal, config)
            return _parse_llm_response(resp_multimodal)
        except Exception as e:
            logging.warning("LLM multimodal request failed: %s", e)
            return None

    else:
        # Multimodal disabled or no image available -> Text Only
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        try:
            resp = _call_llm_chat(messages, config)
            return _parse_llm_response(resp)
        except Exception as e:
            logging.warning("LLM request failed: %s", e)
            return None

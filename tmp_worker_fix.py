if False:
    def _find_subset_match(self, items, target_amt, max_size=8, tolerance=0.05, max_combos=150000):
        """支持近似匹配的子集和搜索 (性能增强版) """
        if not items: return None, 0.0
        from itertools import combinations
        import time
        
        # 优化排序：优先尝试金额较大的项，这对于大额单据的分拆匹配更有利
        search_items = sorted(items, key=lambda x: x['amount'], reverse=True)
        
        # 搜索限制：对于大额目标，扩大搜索范围
        search_limit = 55 if target_amt > 10000 else 35
        search_items = search_items[:search_limit] 
        n = len(search_items)
        best_combo = None
        min_diff = float('inf')
        
        checked = 0
        start_time = time.time()

        for size in range(1, min(n, max_size) + 1):
            if checked > max_combos or time.time() - start_time > 20: break

            for combo in combinations(search_items, size):
                checked += 1
                if checked % 10000 == 0:
                    if checked > max_combos or time.time() - start_time > 25: break

                sum_amt = sum(x['amount'] for x in combo)
                diff = abs(sum_amt - target_amt)
                
                if diff < 0.02: return combo, 0.0 
                
                if diff <= tolerance and diff < min_diff:
                    min_diff = diff
                    best_combo = combo
            
            if checked > max_combos: break
        
        if best_combo:
            actual_sum = sum(x['amount'] for x in best_combo)
            return best_combo, (actual_sum - target_amt)
        return None, 0.0

    def do_reconciliation(self):
        recon_results = []
        used_statement_indices = set()
        used_report_indices = set()
        
        self.progress.emit("正在预热数据并解析特征...")
        for i, st in enumerate(self.statement_records):
            st.pop('_ui_color', None)
            st['_dt'] = safe_parse_date_to_date(st['date'])
            if not st.get('cnpj'):
                found = re.findall(r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})|(\d{3}\.\d{3}\.\d{3}-\d{2})', st.get('desc', ''))
                if found: st['cnpj'] = found[0][0] or found[0][1]
            
            st_cnpj_clean = re.sub(r'\D', '', st.get('cnpj', '')) if st.get('cnpj') else ""
            st_desc = st.get('desc', '')
            st['_std_partner'] = self.mapping_mgr.get_partner_std(st_cnpj_clean) or \
                                 self.mapping_mgr.get_partner_std(st_desc) or st_desc

        for i, rep in enumerate(self.report_records):
            rep.pop('_ui_color', None)
            rep['_dt_pay'] = safe_parse_date_to_date(rep.get('pay_date'))
            rep['_dt_due'] = safe_parse_date_to_date(rep.get('due_date'))
            rep['_dt_best'] = rep['_dt_pay'] or rep['_dt_due']
            std_p = self.mapping_mgr.get_partner_std(rep.get('name', '')) or rep.get('name', '')
            if "SHPP" in rep.get('name', '').upper() and "NORTE" not in std_p.upper():
                std_p = "NORTETOOLS (SHPP)"
            rep['_std_partner'] = std_p

        # --- Phase 1: 1-to-1 Strong Match ---
        self.progress.emit("阶段 1: 正在进行 1对1 精确匹配 (日期优先)...")
        for r_idx, rep in enumerate(self.report_records):
            if r_idx in used_report_indices: continue
            rep_amt = rep['amount'] or 0.0
            dt_rep = rep['_dt_best']
            if not dt_rep: continue
            
            ref_info = CollectionReportParser.parse_invoice_reference(rep['invoice_ref'], self.enable_local_rules)
            clean_num = ref_info.get('invoice') if ref_info else CollectionReportParser.clean_invoice_number(rep['invoice_ref'], self.enable_local_rules)

            best_s_idx = -1
            best_score = -1
            for s_idx, st in enumerate(self.statement_records):
                if s_idx in used_statement_indices: continue
                if abs(st['amount'] - rep_amt) >= 0.05 or (st['amount'] * rep_amt < 0): continue
                sim = calculate_similarity(rep['_std_partner'], st['_std_partner'])
                if ("SHPP" in st['desc'].upper() and "NORTE" in rep['_std_partner'].upper()): sim = max(sim, 0.9)
                date_diff = abs((dt_rep - st['_dt']).days) if st['_dt'] else 999
                max_days = 30 if any(k in st['desc'].upper() for k in ["NORTE", "SHPP", "DANPLER", "ARGOTECH"]) else 12
                if date_diff > max_days: continue
                ref_match = clean_num and clean_num in st.get('desc', '') if clean_num else False
                score = 0
                if ref_match: score += 80
                if sim >= 0.85: score += 40
                elif sim >= 0.6: score += 20
                score -= date_diff * 2.5 
                if score > best_score and score >= 35:
                    best_score = score
                    best_s_idx = s_idx
            if best_s_idx != -1:
                used_report_indices.add(r_idx)
                used_statement_indices.add(best_s_idx)
                recon_results.append({
                    "report": rep, "statement": self.statement_records[best_s_idx],
                    "s_idx": best_s_idx, 
                    "type": "STRONG" if best_score >= 80 else "MEDIUM",
                    "ref_info": ref_info, "clean_num": clean_num
                })

        # --- Phase 1.5: Affinity Pre-Lock ---
        self.progress.emit("阶段 1.5: 正在预锁定强相关流水 (SHPP/NORTE 等)...")
        for s_idx, st in enumerate(self.statement_records):
            if s_idx in used_statement_indices or st['amount'] <= 0: continue
            st_desc = st['desc'].upper()
            st_std = st['_std_partner'].upper()
            target_unit = None
            if "SHPP" in st_desc: target_unit = "NORTE"
            elif "DANPLER" in st_desc: target_unit = "DANPLER"
            elif "ARGOTECH" in st_desc: target_unit = "ARGOTECH"
            elif len(st_std) > 5 and not any(k in st_std for k in ["PIX RECEBIDO", "BOLETOS", "EXTRATO"]):
                target_unit = st_std
            if not target_unit: continue
            for r_idx, rep in enumerate(self.report_records):
                if r_idx in used_report_indices: continue
                rep_std = rep['_std_partner'].upper()
                if target_unit in rep_std or rep_std in target_unit:
                    if abs(rep['amount'] - st['amount']) < 0.05:
                        used_report_indices.add(r_idx)
                        used_statement_indices.add(s_idx)
                        recon_results.append({
                            "report": rep, "statement": st, "s_idx": s_idx, "type": "STRONG",
                            "note": "🎯 强相关单位预锁定"
                        })
                        break

        # --- Phase 2: N-to-1 Aggregate Match ---
        self.progress.emit("阶段 2: 正在搜索聚合入账组合...")
        remaining_stmts_indices = [i for i, s in enumerate(self.statement_records) if i not in used_statement_indices and s['amount'] > 0]
        remaining_stmts_indices.sort(key=lambda i: self.statement_records[i]['_dt'] or datetime.min)

        for s_idx in remaining_stmts_indices:
            st = self.statement_records[s_idx]
            dt_st = st['_dt']
            if not dt_st: continue
            desc_upper = st.get('desc', '').upper()
            st_std = st.get('_std_partner', '').upper()
            search_days_back = 35 if any(k in desc_upper for k in ["NORTE", "PALACIO", "DEYUN", "CAIYA", "ARGOTECH", "GEOLOC", "SHPP", "MARTELUX", "DANPLER"]) else 10
            candidates = []
            for r_idx, rep in enumerate(self.report_records):
                if r_idx in used_report_indices: continue
                rep_std = rep.get('_std_partner', '').upper()
                is_shopee_norte = ("SHPP" in desc_upper and "NORTE" in rep_std)
                if st_std and not any(k in st_std for k in ["BOLETOS RECEBIDOS", "PIX RECEBIDO", "EXTRATO", "TRANSFERENCIA"]):
                    if st_std not in rep_std and rep_std not in st_std and not is_shopee_norte: continue
                dt_rep = rep['_dt_best']
                if dt_rep and -2 <= (dt_st - dt_rep).days <= search_days_back:
                    candidates.append(rep)
            if not candidates: continue
            candidates.sort(key=lambda x: abs((dt_st - x['_dt_best']).days) if x.get('_dt_best') else 999)
            best_subset, batch_diff = self._find_subset_match(candidates, st['amount'], max_size=12, tolerance=0.05, max_combos=150000)
            if best_subset:
                used_statement_indices.add(s_idx)
                is_perfect = abs(batch_diff) < 0.02
                for rep in best_subset:
                    for ridx, robj in enumerate(self.report_records):
                        if robj is rep:
                            used_report_indices.add(ridx)
                            break
                    recon_results.append({
                        "report": rep, "statement": st, "s_idx": s_idx,
                        "type": "BATCH" if is_perfect else "PARTIAL",
                        "note": f"聚合入账: 批次额 {st['amount']:,.2f}",
                        "batch_diff": batch_diff
                    })

        # --- Phase 3: 1-to-N Split Payment Match ---
        self.progress.emit("阶段 3: 正在分析分拆支付项 (深度搜索)...")
        remaining_reps_indices = [i for i in range(len(self.report_records)) if i not in used_report_indices]
        remaining_reps_indices.sort(key=lambda i: self.report_records[i]['amount'], reverse=True)

        for r_idx in remaining_reps_indices:
            rep = self.report_records[r_idx]
            if rep['amount'] < 500: continue
            is_ultra = rep['amount'] > 50000
            max_w = 35 if (is_ultra or "NORTE" in rep['_std_partner'].upper()) else 15
            
            candidates_with_idx = []
            rep_std = rep['_std_partner'].upper()
            for s_idx, s in enumerate(self.statement_records):
                if s_idx in used_statement_indices or s['amount'] <= 0: continue
                if not s['_dt'] or abs((s['_dt'] - rep['_dt_best']).days) > max_w: continue
                s_std = s.get('_std_partner', '').upper()
                s_desc = s['desc'].upper()
                is_shopee_norte = ("SHPP" in s_desc and "NORTE" in rep_std)
                has_clear_unit = any(k in s_std for k in ["BOLETOS RECEBIDOS", "PIX RECEBIDO", "EXTRATO", "TRANSFERENCIA"]) is False and len(s_std) > 3
                if has_clear_unit:
                    if s_std in rep_std or rep_std in s_std or is_shopee_norte: candidates_with_idx.append((s_idx, s))
                else:
                    w_limit = 30 if is_ultra else 10
                    if abs((s['_dt'] - rep['_dt_best']).days) <= w_limit: candidates_with_idx.append((s_idx, s))
            
            if not candidates_with_idx: continue
            c_limit = 1200000 if is_ultra else 300000
            search_tol = 15.0 if is_ultra else 0.10
            candidate_objs = [x[1] for x in candidates_with_idx]
            best_stmt_subset, stmt_diff = self._find_subset_match(candidate_objs, rep['amount'], max_size=15, tolerance=search_tol, max_combos=c_limit)
            
            if best_stmt_subset:
                used_report_indices.add(r_idx)
                is_perfect = abs(stmt_diff) < 0.10
                for st_obj in best_stmt_subset:
                    matched_s_idx = -1
                    for o_idx, s_obj in candidates_with_idx:
                        if s_obj is st_obj:
                            matched_s_idx = o_idx
                            break
                    if matched_s_idx != -1:
                        used_statement_indices.add(matched_s_idx)
                        recon_results.append({
                            "report": rep, "statement": st_obj, "s_idx": matched_s_idx,
                            "type": "SPLIT" if is_perfect else "SPLIT_PARTIAL",
                            "note": f"🚀 分拆匹配 (深): 误差 {stmt_diff:,.2f}",
                            "batch_diff": stmt_diff
                        })

        # --- Phase 4: Standard Fallback ---
        self.progress.emit("阶段 4: 兜底金额模糊匹配...")
        for r_idx, rep in enumerate(self.report_records):
            if r_idx in used_report_indices: continue
            rep_amt = rep['amount'] or 0.0
            dt_rep = rep['_dt_best']
            for s_idx, st in enumerate(self.statement_records):
                if s_idx in used_statement_indices: continue
                if abs(st['amount'] - rep_amt) < 0.05:
                    date_diff = abs((dt_rep - st['_dt']).days) if dt_rep and st['_dt'] else 999
                    if date_diff <= 7:
                        used_report_indices.add(r_idx)
                        used_statement_indices.add(s_idx)
                        recon_results.append({
                            "report": rep, "statement": st, "s_idx": s_idx, "type": "SUSPECT",
                            "note": f"金额模糊锁定"
                        })
                        break

        # Final Wrap-up
        for r_idx, rep in enumerate(self.report_records):
            if r_idx in used_report_indices: continue
            recon_results.append({ "report": rep, "statement": None, "s_idx": -1, "type": "NONE" })
            
        return recon_results

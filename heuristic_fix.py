if False:
    def _find_subset_match(self, items, target_amt, max_size=8, tolerance=0.05):
        """
        寻找子集和匹配。
        支持两种模式：
        1. 精确匹配 (tolerance < 1.0)
        2. 近似匹配 (返回最接近的子集)
        """
        if not items: return None, 0.0
        
        from itertools import combinations
        
        # 限制搜索深度，优先选择最近的记录
        search_items = items[:25] 
        n = len(search_items)
        
        best_combo = None
        min_diff = float('inf')

        # 1. 尝试寻找精确匹配 (误差极小)
        for size in range(1, min(n, max_size) + 1):
            for combo in combinations(search_items, size):
                sum_amt = sum(x['amount'] for x in combo)
                diff = abs(sum_amt - target_amt)
                
                if diff < tolerance:
                    return combo, 0.0 # 精确匹配直接返回
                
                # 记录最接近的组合
                if diff < min_diff:
                    min_diff = diff
                    best_combo = combo
        
        # 2. 如果没找到精确匹配，且 min_diff 在合理范围内 (例如 < 100 且 差异率 < 5%)，则作为疑似组合返回
        if best_combo and (min_diff < 100 or (target_amt > 0 and min_diff / target_amt < 0.05)):
            # 计算总额差异
            actual_sum = sum(x['amount'] for x in best_combo)
            return best_combo, (actual_sum - target_amt)
            
        return None, 0.0

    def run_reconciliation(self):
        """深化自动核对模块：高效聚合核对算法，支持带差额的近似组合匹配"""
        if not self.report_records:
            QMessageBox.warning(self, "缺失数据", "请先导入回单报告记录")
            return
            
        self.table.setRowCount(0)
        self.table.setSortingEnabled(False)
        
        recon_results = []
        used_statement_indices = set()
        used_report_indices = set()
        
        # 1. 数据预热：单位归一化与日期解析
        for st in self.statement_records:
            st.pop('_ui_color', None)
            st['_dt'] = safe_parse_date_to_date(st['date'])
            if not st.get('cnpj'):
                found = re.findall(r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})', st.get('desc', ''))
                if found: st['cnpj'] = found[0]
            st_cnpj_clean = re.sub(r'\D', '', st.get('cnpj', '')) if st.get('cnpj') else ""
            st['_std_partner'] = self.main_window.mapping_mgr.get_partner_std(st_cnpj_clean) or \
                                 self.main_window.mapping_mgr.get_partner_std(st.get('desc', '')) or st.get('desc', '')

        for rep in self.report_records:
            rep.pop('_ui_color', None)
            rep['_dt_pay'] = safe_parse_date_to_date(rep.get('pay_date'))
            rep['_dt_due'] = safe_parse_date_to_date(rep.get('due_date'))
            rep['_dt_best'] = rep['_dt_pay'] or rep['_dt_due']
            rep['_std_partner'] = self.main_window.mapping_mgr.get_partner_std(rep.get('name', '')) or rep.get('name', '')

        # --- Phase 1: 1-to-1 Strong Match (Confidence > 70) ---
        for r_idx, rep in enumerate(self.report_records):
            rep_amt = rep['amount'] or 0.0
            dt_rep = rep['_dt_best']
            enable_local = QSettings(SETTINGS_FILE, QSettings.IniFormat).value("enable_local_rules", True, type=bool)
            ref_info = CollectionReportParser.parse_invoice_reference(rep['invoice_ref'], enable_local)
            clean_num = ref_info.get('invoice') if ref_info else CollectionReportParser.clean_invoice_number(rep['invoice_ref'], enable_local)

            best_s_idx = -1
            best_score = -1
            for s_idx, st in enumerate(self.statement_records):
                if s_idx in used_statement_indices: continue
                if abs(st['amount'] - rep_amt) >= 0.05 or (st['amount'] * rep_amt < 0): continue
                date_diff = abs((dt_rep - st['_dt']).days) if dt_rep and st['_dt'] else 999
                if date_diff > 10: continue
                sim = calculate_similarity(rep['_std_partner'], st['_std_partner'])
                ref_match = clean_num and clean_num in st.get('desc', '') if clean_num else False
                score = 0
                if ref_match: score += 60
                if sim >= 0.8: score += 40
                elif sim >= 0.5: score += 20
                score -= date_diff * 2
                if score > best_score and score >= 30:
                    best_score = score
                    best_s_idx = s_idx
            
            if best_s_idx != -1:
                used_report_indices.add(r_idx)
                used_statement_indices.add(best_s_idx)
                recon_results.append({
                    "report": rep, "statement": self.statement_records[best_s_idx],
                    "type": "STRONG" if best_score >= 70 else "MEDIUM",
                    "ref_info": ref_info, "clean_num": clean_num
                })

        # --- Phase 2: Aggregate Match (N-to-1) ---
        remaining_stmts = [i for i, s in enumerate(self.statement_records) if i not in used_statement_indices and s['amount'] > 0]
        remaining_stmts.sort(key=lambda i: self.statement_records[i]['_dt'] or datetime.min)

        for s_idx in remaining_stmts:
            st = self.statement_records[s_idx]
            dt_st = st['_dt']
            if not dt_st: continue
            
            # 筛选前 5 天内的报告
            candidates = []
            for r_idx, rep in enumerate(self.report_records):
                if r_idx in used_report_indices: continue
                dt_rep = rep['_dt_best']
                if dt_rep and -1 <= (dt_st - dt_rep).days <= 5:
                    candidates.append(rep)
            
            if not candidates: continue
            
            best_subset, batch_diff = self._find_subset_match(candidates, st['amount'], max_size=10, tolerance=0.05)
            
            if best_subset:
                used_statement_indices.add(s_idx)
                is_perfect = abs(batch_diff) < 0.05
                for rep in best_subset:
                    for r_idx, r_obj in enumerate(self.report_records):
                        if r_obj is rep:
                            used_report_indices.add(r_idx)
                            enable_local = QSettings(SETTINGS_FILE, QSettings.IniFormat).value("enable_local_rules", True, type=bool)
                            recon_results.append({
                                "report": rep, "statement": st, 
                                "type": "BATCH" if is_perfect else "PARTIAL",
                                "ref_info": CollectionReportParser.parse_invoice_reference(rep['invoice_ref'], enable_local),
                                "clean_num": CollectionReportParser.clean_invoice_number(rep['invoice_ref'], enable_local),
                                "note": f"聚合入账 {'(含差额)' if not is_perfect else ''}: 批次总额 {st['amount']:,.2f}",
                                "batch_diff": batch_diff
                            })
                            break

        # --- Phase 3: Suspect (Amount Only) ---
        for r_idx, rep in enumerate(self.report_records):
            if r_idx in used_report_indices: continue
            rep_amt = rep['amount'] or 0.0
            dt_rep = rep['_dt_best']
            for s_idx, st in enumerate(self.statement_records):
                if s_idx in used_statement_indices: continue
                if abs(st['amount'] - rep_amt) < 0.05:
                    date_diff = abs((dt_rep - st['_dt']).days) if dt_rep and st['_dt'] else 999
                    if date_diff <= 2:
                        used_report_indices.add(r_idx)
                        used_statement_indices.add(s_idx)
                        enable_local = QSettings(SETTINGS_FILE, QSettings.IniFormat).value("enable_local_rules", True, type=bool)
                        recon_results.append({
                            "report": rep, "statement": st, "type": "SUSPECT",
                            "ref_info": CollectionReportParser.parse_invoice_reference(rep['invoice_ref'], enable_local),
                            "clean_num": CollectionReportParser.clean_invoice_number(rep['invoice_ref'], enable_local)
                        })
                        break

        # --- Phase 4: Final Cleanup (Unmatched) ---
        for r_idx, rep in enumerate(self.report_records):
            if r_idx in used_report_indices: continue
            enable_local = QSettings(SETTINGS_FILE, QSettings.IniFormat).value("enable_local_rules", True, type=bool)
            recon_results.append({
                "report": rep, "statement": None, "type": "NONE",
                "ref_info": CollectionReportParser.parse_invoice_reference(rep['invoice_ref'], enable_local),
                "clean_num": CollectionReportParser.clean_invoice_number(rep['invoice_ref'], enable_local)
            })

        # --- UI 填充与配色 ---
        COLOR_STRONG = QColor("#dff0d8") 
        COLOR_MEDIUM = QColor("#e8f5e9")
        COLOR_BATCH = QColor("#d9edf7")
        COLOR_PARTIAL = QColor("#fff3e0") 
        COLOR_SUSPECT = QColor("#fcf8e3")
        COLOR_NONE = QColor("#f2dede")

        self.table.setRowCount(len(recon_results))
        for i, res in enumerate(recon_results):
            rep, st, m_type = res['report'], res['statement'], res['type']
            chk_item = QTableWidgetItem()
            chk_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            chk_item.setCheckState(Qt.Checked if m_type in ["STRONG", "MEDIUM", "BATCH", "PARTIAL"] else Qt.Unchecked)
            self.table.setItem(i, 0, chk_item)

            bg_color = COLOR_NONE
            status_txt = "❌ 缺失流水"
            diff_val = (rep['amount'] - st['amount']) if st else rep['amount']
            stmt_amt_display = f"{st['amount']:,.2f}" if st else "-"

            if m_type == "STRONG": status_txt, bg_color = "✅ 完美匹配", COLOR_STRONG
            elif m_type == "MEDIUM": status_txt, bg_color = "🟢 智能匹配", COLOR_MEDIUM
            elif m_type == "BATCH": 
                status_txt, bg_color = "📦 组合匹配", COLOR_BATCH
                stmt_amt_display += " (组合)"
                diff_val = 0.0
            elif m_type == "PARTIAL":
                status_txt, bg_color = "🧩 部分匹配 (组合)", COLOR_PARTIAL
                stmt_amt_display += " (组合)"
                diff_val = res.get('batch_diff', 0.0)
            elif m_type == "SUSPECT": status_txt, bg_color = "❓ 疑似匹配", COLOR_SUSPECT

            rep['_ui_color'] = bg_color
            if st: st['_ui_color'] = bg_color
            
            row_data = [
                status_txt, rep['_std_partner'],
                f"{res.get('clean_num', '')} (Raw: {rep['invoice_ref']})" if res.get('clean_num') else rep['invoice_ref'],
                format_date_gui(rep.get('due_date', '')), format_date_gui(rep.get('pay_date', '')),
                f"{rep['amount']:,.2f}", format_date_gui(st['date']) if st else "-",
                stmt_amt_display, f"{diff_val:,.2f}", (res.get('note') or (st['desc'] if st else "(未匹配)"))
            ]
            for col_idx, val in enumerate(row_data):
                item = QTableWidgetItem(str(val))
                item.setBackground(bg_color)
                if col_idx in [5, 7, 8]: item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.table.setItem(i, col_idx + 1, item)
                chk_item.setData(Qt.UserRole, res)

        # 填充流水视图 (同步更新颜色)
        stmt_map = {}
        for res in recon_results:
            if res['statement']:
                sid = id(res['statement'])
                if sid not in stmt_map: stmt_map[sid] = {'type': res['type'], 'count': 0, 'reps': []}
                stmt_map[sid]['count'] += 1
                stmt_map[sid]['reps'].append(res['report']['name'])
        
        self.stmt_table.setRowCount(len(self.statement_records))
        for i, st in enumerate(self.statement_records):
            info = stmt_map.get(id(st))
            status_str, bg_color = (f"✅ 已匹配 ({info['type']})", QColor("#dff0d8")) if info else ("❌ 未匹配", QColor("#ffffff"))
            st['_ui_color'] = bg_color if info else None
            match_info = (", ".join(info['reps'][:2]) + ("..." if len(info['reps'])>2 else "")) if info else "-"
            row_data = [status_str, format_date_gui(st['date']), st['desc'], f"{st['amount']:,.2f}", st.get('cnpj', ''), match_info]
            for c_idx, val in enumerate(row_data):
                item = QTableWidgetItem(str(val))
                item.setBackground(bg_color)
                self.stmt_table.setItem(i, c_idx, item)
                if c_idx == 0: item.setData(Qt.UserRole, st)

        self.recon_results = recon_results
        self.btn_batch_confirm.setEnabled(len(recon_results) > 0)
        self.auto_save_results(recon_results)
        QMessageBox.information(self, "核对完成", f"对账结束，已通过启发式子集搜索吸纳带差额的组合匹配项。")

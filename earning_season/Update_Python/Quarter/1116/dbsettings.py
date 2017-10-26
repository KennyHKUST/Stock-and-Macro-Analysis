# coding=utf-8

# settings for the database
RAW_HOST = '127.0.0.1'
RAW_USER = 'root'
RAW_PASS = 'yang83546852A'
DB_NAME = 'trweb'
REC_TBNAME = 'tr_insert_record'
TB_NAME = 'tr_report_annual'

rootDir = u"C:/project/Lixi/raw/Annual"
annualFolder = range(10)
sqlcolDict = {
    # 1:"fy",
    # 2:"tot_rev",
    # 3:"sga_exp_tot",
    # 4:"cost_rev_tot",
    # 5:"netinc_after_tax",
    # 6:"tot_com_share_ostd",
    # 7:"tot_pref_share_ostd",
    # 8:"acc_rec_trade",
    # 9:"acc_pay",
    # 10:"tot_invent",
    # 11:"tot_asset_rep",
    # 12:"tot_cur_asset",
    # 13:"cash_shortterm_invest",
    # 14:"accrued_exp",
    # 15:"tot_longterm_debt",
    # 16:"tot_debt",
    # 17:"tot_equity",
    # 18:"cap_lease",
    # 19:"tot_liability",
    # 20:"tot_cur_liability",
    # 21:"tot_property",
    # 22:"cash_operating",
    # 23:"cash_finance",
    # 24:"cash_invest",
    # 25:"foreign_exch",
    # 26:"cash_divid_paid",
    # 27:"historic_pe",
    # 28:"historic_ev",
    # 29:"bookval_pershare",
    # 30:"netinc_before_extra",
    1:"tot_rev",  
    2:"cost_rev_tot",
    3:"tot_com_share_ostd",
    4:"tot_pref_share_ostd",
    5:"acc_rec_trade",
    6:"acc_pay",
    7:"tot_invent",
    8:"tot_asset_rep",
    9:"tot_cur_asset",
    10:"cash_shortterm_invest",
    11:"accrued_exp",
    12:"tot_longterm_debt",
    13:"tot_debt",
    14:"tot_equity",
    15:"cap_lease",
    16:"tot_liability",
    17:"tot_cur_liability",
    18:"tot_property",
    19:"cash_operating",
    20:"cash_finance",
    21:"cash_invest",
    22:"foreign_exch",
    23:"cash_divid_paid",
    24:"historic_pe",
    25:"bookval_pershare",
    26:"earning_pershare",
    27:"netinc_before_extra",
    28:"netinc_inextra_bedist",
    29:"sga_exp_tot",
}    
colDict = {
    # 1:"fy",
    # 2:"tot_rev",
    # 3:"cost_rev_tot",
    # 4:"tot_com_share_ostd",
    # 5:"tot_pref_share_ostd",
    # 6:"acc_rec_trade",
    # 7:"acc_pay",
    # 8:"tot_invent",
    # 9:"tot_asset_rep",
    # 10:"tot_cur_asset",
    # 11:"cash_shortterm_invest",
    # 12:"accrued_exp",
    # 13:"tot_longterm_debt",
    # 14:"tot_debt",
    # 15:"tot_equity",
    # 16:"cap_lease",
    # 17:"tot_liability",
    # 18:"tot_cur_liability",
    # 19:"tot_property",
    # 20:"cash_operating",
    # 21:"cash_finance",
    # 22:"cash_invest",
    # 23:"foreign_exch",
    # 24:"cash_divid_paid",
    # 25:"historic_pe",
    # 26:"bookval_pershare",
    # 27:"historic_ev",
    # 28:"netinc_after_tax",
    # 29:"netinc_before_extra",
    # 30:"sga_exp_tot"
    1:"tot_rev",
    2:"sga_exp_tot",
    3:"cost_rev_tot",
    4:"netinc_inextra_bedist",
    5:"tot_com_share_ostd",
    6:"tot_pref_share_ostd",
    7:"acc_rec_trade",
    8:"acc_pay",
    9:"tot_invent",
    10:"tot_asset_rep",
    11:"tot_cur_asset",
    12:"cash_shortterm_invest",
    13:"accrued_exp",
    14:"tot_longterm_debt",
    15:"tot_debt",
    16:"tot_equity",
    17:"cap_lease",
    18:"tot_liability",
    19:"tot_cur_liability",
    20:"tot_property",
    21:"cash_operating",
    22:"cash_finance",
    23:"cash_invest",
    24:"foreign_exch",
    25:"cash_divid_paid",
    26:"historic_pe",
    27:"bookval_pershare",
}   
numCol = len(colDict)


    
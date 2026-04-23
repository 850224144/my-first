# -*- coding: utf-8 -*-

def get_all_stocks():
    """
    A股全市场股票池（工程稳定版）
    不依赖第三方接口
    """

    stocks = []

    # 上证
    stocks += [f"600{i:03d}" for i in range(1, 999)]
    stocks += [f"601{i:03d}" for i in range(1, 999)]
    stocks += [f"603{i:03d}" for i in range(1, 999)]
    stocks += [f"605{i:03d}" for i in range(1, 999)]

    # 深证主板
    stocks += [f"000{i:03d}" for i in range(1, 999)]
    stocks += [f"001{i:03d}" for i in range(1, 999)]

    # 创业板
    stocks += [f"300{i:03d}" for i in range(1, 999)]

    # 科创板
    stocks += [f"688{i:03d}" for i in range(1, 999)]

    # 去重 + 过滤
    stocks = list(set([s for s in stocks if len(s) == 6]))

    return stocks
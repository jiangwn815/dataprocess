import pandas as pd
import numpy as np


def get_product_type(df):
    product_type_list = ("有余卡",  "天翼不限量129元套餐", "天翼不限量",
                         "磅礴卡", "日租卡", "大王卡",
                         "iFree+", "iFree4G", "先锋卡",
                         "人悦卡")
    k = df["商品名称"]
    product_type = "未匹配"
    for p in product_type_list:
        if p in k:
            product_type = p
    return product_type


def add_product_type(df):
    pt = df.apply(get_product_type, axis=1)
    df["产品类型"] = pt


def get_manager(df):
    manager_mapping_list = {
        "蒋伟男": ("上海缘聚","北京乐卡","南京移宝","落基伟业","天为畅游"),
        "李天阳": ("广州粤亮", "京东商城", "中国邮电器材", "苏宁"),
        "刘勇": ("盛丰伟业", "北京京科通讯", "泰龙吉", "易店铺"),
        "宋飞": ("北京光迅互联", "北京吉利创想", "珠峰讯", "天恒信通", "蚂蚁聚力"),
        "赵德隆": ("济南八骏", "亚信科技", "北京捷康特光", "天拓数信", "山东尊为", "长沙时器")
    }

    agent_target = df["代理商名称"]
    manager = "未匹配"
    for name, agents_list in manager_mapping_list.items():
        for agent in agents_list:
            if agent_target == agent:
                manager = name
    return manager


def add_manager(df):
    manager_series = df.apply(get_manager, axis=1)
    df["渠道经理"] = manager_series


def add_manager2(df):
    manager_mapping_list2 = {
        "上海缘聚": "蒋伟男",
        "北京乐卡": "蒋伟男",
        "南京移宝": "蒋伟男",
        "落基伟业": "蒋伟男",
        "天为畅游": "蒋伟男",
        "广州粤亮": "李天阳",
        "京东商城": "李天阳",
        "中国邮电器材": "李天阳",
        "苏宁": "李天阳",
        "济南八骏": "赵德隆",
        "亚信科技": "赵德隆",
        "北京捷康特光": "赵德隆",
        "天拓数信": "赵德隆",
        "山东尊为": "赵德隆",
        "长沙时器": "赵德隆",
        "北京光迅互联": "宋飞",
        "北京吉利创想": "宋飞",
        "珠峰讯": "宋飞",
        "天恒信通": "宋飞",
        "蚂蚁聚力": "宋飞"
    }
    df["渠道经理2"] = df["代理商名称"].map(manager_mapping_list2)


def to_excel_utf8(df, filename):
    df.to_excel(filename+".xls", encoding="utf_8_sig", index=None)


def read_agent_file(filetype, date_start, date_end, year_start="2018", year_end="2018", ext="xls"):
    file_date_part = year_start+date_start+"-"+year_end+date_end+"."+ext
    filename = ""
    if filetype == "sent":
        filename = "代理商已发货数据"+file_date_part
    elif filetype == "mobile":
        filename = "代理商号码" + file_date_part
    df = pd.read_excel(filename)
    add_manager(df)
    if filetype == "sent":
        add_product_type(df)
        df = df[df["订单状态"] != "订单返销"]
        df.drop_duplicates(["流转系统订单号", "业务号码", "订单状态"])
    return df


def divide_file(filename, col_name):
    df = pd.read_excel(filename)
    items = df[col_name].unique()
    for item in items:
        divided_data = df[df[col_name] == item]
        divided_filename = filename.split(".")[0]+"拆分-"+str(item)
        to_excel_utf8(divided_data,divided_filename)


def convert_to_float(df):
    df = df.applymap(lambda x: str(x).replace(",", ""))
    df = pd.DataFrame(df, dtype=np.float)
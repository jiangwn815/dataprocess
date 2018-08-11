def get_product_type(df):
    product_type_list = ("有余卡", "天翼不限量", "磅礴卡", "日租卡", "大王卡", "iFree+", "iFree4G", "先锋卡")
    for p in product_type_list:
        k = df["商品名称"]
    return p


def add_product_type(df):
    pt = get_product_type(df)
    df["产品类型"] = pt

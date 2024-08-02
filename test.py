import efinance as ef
# ef.stock.get_quote_history('600519', beg='20230101' ,end='20500101', klt=15, return_df=True)
dd=ef.stock.get_quote_history('600519')
print(dd)
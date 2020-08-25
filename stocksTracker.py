import concurrent.futures as cf
import pandas as pd
import requests
import bs4


# 2 files as data source
# - stockLinks.xlxs (contains links to the shares)
# - stockPricesWeekly.xlxs (contains the weekly stocks)

BASEFILE = "StocksBaseFile.xlsx"
GENERATIONFILE = "StocksGeneratedFile.xlsx"


def getStockValue(link, field='Close*'):
    # pass field="Date" to get dates
    print("INFO: Sending Request for data")
    data = requests.get(link)
    soup = bs4.BeautifulSoup(data.content, 'html.parser')
    soups = soup.find_all('table')

    for soup in soups:
        try:
            if "/history" not in link:
                print(f'ERROR: Invalid link for search: {link}')
            # the 0 since read_html returns a list
            df = pd.read_html(soup.prettify())[0]
            print("INFO: returning data")
            try:
                values = list(df[field][:7])
            except KeyError:
                print(df.head())
                print(f"ERROR: Link= {link}")
            if field == "Date":
                values = list(map(lambda x: x.strip(), values))
            else:
                try:
                    values = list(map(lambda x: float(x), values))
                except ValueError:
                    pass
        except:
            continue
    return [link]+values


basefile = pd.read_excel(BASEFILE)
dates = getStockValue(
    "https://in.finance.yahoo.com/quote/KARURVYSYA.NS/history/", field="Date"
)
columns = ['Name', '52 wk low', '52 wk high']+dates
df = pd.DataFrame(columns=columns)
count = 0
data = {}

print("INFO: Starting thread pool executor")
with cf.ThreadPoolExecutor() as executor:
    results = [executor.submit(getStockValue, row["Links"])
               for _, row in basefile.iterrows()]
    for value in cf.as_completed(results):
        value = value.result()
        data[value[0]] = value[1:]
        count += 1
        print(f"Completed: {count}")

for _, row in basefile.iterrows():
    new = [row["Name"], row['52 wk low'], row['52 wk high']]
    new += data[row["Links"]]
    df = df.append(dict(zip(columns, new)), ignore_index=True)


df.to_excel(GENERATIONFILE)

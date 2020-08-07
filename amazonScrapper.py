import requests
import concurrent.futures as cf
import CSV_Files as csv
from bs4 import BeautifulSoup


search_url = "https://www.amazon.in/s?k=pencils&ref=nb_sb_noss_2"
save_file = 'x-AmazonWebscrapper.csv'

def get_search_results(search_url):
    page = 1
    base_url = (
        search_url
    )

    data = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"})
    data = BeautifulSoup(data.content, 'html.parser')
    data = data.find_all("div", class_='sg-col-inner')

    urls = []

    for item in data:
        name = item.find_all(
            'span',
            class_="a-size-base-plus a-color-base a-text-normal"
        )
        if len(name) > 0:
            name = name[0].contents[0]
        else:
            print('item without name')
            continue

        item_url = item.find_all(
            'a', class_="a-link-normal s-no-outline")[0]["href"]
        if "slredirect" not in item_url:
            urls.append('https://www.amazon.in'+item_url)

    return urls


def get_item_amazon(url):
    data = BeautifulSoup(requests.get(
        url, headers={"User-Agent": "Defined"}
    ).content, 'html.parser')

    items = {
        'title': 'productTitle',
        'price': 'priceblock_ourprice',
        'image': 'landingImage',
        'vendor': 'bylineInfo',
        'customerReviews': 'acrCustomerReviewText'
    }

    nums = '1234567890.'

    for item in items:
        items[item] = data.find(id=items[item])
        if items[item] is None:
            return None

        if item == 'title':
            print(items[item].contents[0].strip(), "completed")

        if item == 'image':
            string = items[item].get('data-a-dynamic-image')
            new = ''
            start = ''
            for char in string:
                if char in ('"', "'") and start:
                    break
                if start:
                    new += char
                if char in ('"', "'") and not start:
                    start = char
            items[item] = new
        elif item == 'price':
            items[item] = items[item].contents[0].strip()
            new = ''
            for char in items[item]:
                if char in nums:
                    new += char
            new = str(float(new))
            items[item] = new
        else:
            items[item] = items[item].contents[0].strip()

    mapping = {
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '1-5': 1.5,
        '2-5': 2.5,
        '3-5': 3.5,
        '4-5': 4.5
    }
    stars = 0
    stars_parent_tag = data.find_all(
        'a', class_='a-popover-trigger a-declarative')[0]
    for item in stars_parent_tag.i.get("class"):
        if 'a-star-' in item:
            stars = item.replace('a-star-', '')
            stars = mapping[stars]
            break

    items['stars'] = str(stars)
    return items


def get_data_from_urls(urls):
    data = []
    with cf.ThreadPoolExecutor() as executor:
        results = [executor.submit(get_item_amazon, url) for url in urls]
        for value in cf.as_completed(results):
            value = value.result()
            if value is not None:
                data.append(value)
    return data



urls = []
while not urls:
    urls = get_search_results(search_url)
    print(len(urls))

print("Completed URLS")
print(urls)

data = get_data_from_urls(urls)
print("Saving to file")

csv_data = []
titles = ['title', 'price', 'image', 'vendor', 'customerReviews', 'stars']
csv_data.append(titles)
for item in data:
    new_item = []
    for title in titles:
        new_item.append(item[title])

    csv_data.append(new_item)

csv.write_csv(csv_data, save_file)

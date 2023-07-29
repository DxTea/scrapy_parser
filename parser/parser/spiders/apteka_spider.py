import os
import time
from abc import ABC

import scrapy
import random
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
from urllib.parse import urljoin
from tqdm import tqdm


# Определение класса для хранения данных о продукте
class AptekaProductItem(scrapy.Item):
    timestamp = scrapy.Field(output_processor=TakeFirst())  # метка времени, когда был собран данный продукт.
    RPC = scrapy.Field(output_processor=TakeFirst())  # уникальный идентификатор продукта.
    url = scrapy.Field(output_processor=TakeFirst())  # URL-адрес продукта.
    title = scrapy.Field(output_processor=TakeFirst())  # заголовок продукта.
    marketing_tags = scrapy.Field(input_processor=MapCompose(str.strip),
                                  output_processor=TakeFirst())  # маркетинговые теги продукта.
    brand = scrapy.Field(output_processor=TakeFirst())  # бренд продукта.
    section = scrapy.Field()  # раздел, к которому принадлежит продукт.
    current_price = scrapy.Field(output_processor=TakeFirst())  # текущая цена продукта.
    original_price = scrapy.Field(output_processor=TakeFirst())  # оригинальная цена продукта.
    sale_tag = scrapy.Field(output_processor=TakeFirst())  # информация о скидке на продукт.
    in_stock = scrapy.Field(output_processor=TakeFirst())  # наличие продукта на складе или в аптеке.
    count = scrapy.Field(output_processor=TakeFirst())  # количество продуктов (не обрабатывается).
    main_image = scrapy.Field(output_processor=TakeFirst())  # URL-адрес основного изображения продукта.
    set_images = scrapy.Field()  # список дополнительных изображений продукта.
    view360 = scrapy.Field()  # 360 видео продукта (не обрабатывается)
    video = scrapy.Field()  # видео продукта (не обрабатывается).
    description = scrapy.Field(output_processor=TakeFirst())  # описание продукта.
    country_of_origin = scrapy.Field(output_processor=TakeFirst())  # страна производителя продукта.


# Определение класса для парсинга данных с веб-страниц
class AptekaSpider(scrapy.Spider, ABC):
    name = "apteka_spider"
    custom_settings = {
        'DOWNLOAD_DELAY': 0.1,  # Задержка
        'CONCURRENT_REQUESTS': 2,  # Кол-во параллельных запросов на сайт
        'PROXY_POOL_ENABLED': True,
        'LOG_LEVEL': 'ERROR',  # Логируем только ошибки
        'USER_AGENT_LIST': [  # Список случайных User-Agent для использования
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 '
            'Safari/537.36 Edge/16.16299',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 '
            'Safari/537.36 Edge/16.16299 '
        ],
    }

    def __init__(self, *args, **kwargs):
        super(AptekaSpider, self).__init__(*args, **kwargs)
        self.item_count = 0

    CATEGORIES = [
        'sredstva-gigieny/uhod-za-polostyu-rta/zubnye-niti_-ershiki',
        'perevyazochnye-sredstva/marlya',
        'medikamenty-i-bady/allergiya/allergiya-vzroslym',
    ]

    # Отправка запросов на веб-страницы для начала парсинга
    def start_requests(self):
        for category in self.CATEGORIES:
            url = f'https://apteka-ot-sklada.ru/catalog/{category}'
            headers = self.get_random_headers()
            yield scrapy.Request(url, headers=headers, callback=self.parse_category,
                                 meta={'category': category,'globalcityid': '92', 'globalcityname': 'Томск',
                                       'location': 'Томская область Томск'})

    # Функция для возврата случайного User-Agent и Referer
    def get_random_headers(self):
        user_agent = random.choice(self.settings['USER_AGENT_LIST'])
        referer = 'https://www.google.com/'  # Здесь можно указать случайный Referer, если необходимо
        return {'User-Agent': user_agent, 'Referer': referer}

    # Парсинг категории с продуктами
    def parse_category(self, response):
        product_links = response.css(
            'div.goods-card__name.text.text_size_default.text_weight_medium > a::attr(href)').getall()
        base_url = response.url
        category = response.meta['category']
        # Пагинация
        pages = 1
        next_page = response.css('.ui-pagination__link_direction::attr(href)').get()
        if next_page and not next_page.endswith('start=0'):
            next_page_url = urljoin(base_url, next_page)
            pages += 1
            yield scrapy.Request(next_page_url, self.parse_category,
                                 meta={'category': category,'globalcityid': '92', 'globalcityname': 'Томск',
                                       'location': 'Томская область Томск'})
        with tqdm(total=len(product_links), unit=' products', desc=f'Category {category} Page № {pages}') as progress_bar:
            for link in product_links:
                full_url = urljoin(base_url, link)
                yield scrapy.Request(full_url, self.parse_product,
                                     meta={'category': category,'globalcityid': '92', 'globalcityname': 'Томск',
                                           'location': 'Томская область Томск'})
                progress_bar.update(1)

    # Проверка наличия товара в аптеках
    @staticmethod
    def in_stock_in_pharmacy(response):
        return bool(response.xpath('//span[contains(text(), "Смотреть в аптеках")]'))

    # Проверка наличия товара на сайте
    @staticmethod
    def in_stock_on_site(response):
        return bool(response.xpath('//span[contains(text(), "Добавить в корзину")]'))

    # Проверка наличия скидки
    @staticmethod
    def is_sale(response):
        return bool(response.xpath("//span[contains(text(), 'STOP Цена')]"))

    # Расчет цен на продукт
    def calculate_price(self, response):
        original_price = 0.0
        sale_price = 0.0

        if self.in_stock_on_site(response) and self.is_sale(response):
            original_price = float(response.xpath(
                '/html/body/div[1]/div/div/div[3]/main/section[1]/div/aside/div/div[1]/div[1]/div[2]/span[2]/text()')
                                   .get().strip().replace(" ", "").replace("₽", ""))
            sale_price = float(response.xpath(
                '/html/body/div[1]/div/div/div[3]/main/section[1]/div/aside/div/div[1]/div[1]/div[2]/span[1]/text()')
                               .get().strip().replace(" ", "").replace("₽", ""))
        elif self.in_stock_on_site(response) and not self.is_sale(response):
            original_price = float(response.xpath(
                "/html/body/div[1]/div/div/div[3]/main/section[1]/div/aside/div/div[1]/div[1]/div[2]/span/text()")
                                   .get().strip().replace(" ", "").replace("₽", ""))
            sale_price = original_price
        elif not self.in_stock_on_site(response) and self.in_stock_in_pharmacy(response):
            original_price = float(response.xpath(
                "/html/body/div[1]/div/div/div[3]/main/section[1]/div/aside/div/div[1]/ul/li/a/span/span/text()")
                                   .get().strip().replace(" ", "").replace("₽", "").replace("от", ""))
            sale_price = original_price

        return original_price, sale_price

    # Расчет процента скидки
    @staticmethod
    def calculate_sale(prices):
        original_price = prices[0]
        sale_price = prices[1]

        if original_price == 0.0:
            return 0.0

        return (1 - (sale_price / original_price)) * 100

    # Парсинг информации о продукте
    def parse_product(self, response):

        loader = ItemLoader(item=AptekaProductItem(), response=response)

        # Добавляем try-except блок для обработки возможных исключений
        with tqdm(total=18, unit=' fields', desc=f'Parsing product: {response.url}') as progress_bar:
            try:
                prices = self.calculate_price(response)
                description = self.get_description(response)

                # Загружаем данные в ItemLoader
                loader.add_value("timestamp", int(time.time()))
                progress_bar.update(1)
                loader.add_value("RPC", response.url.split("_")[-1])
                progress_bar.update(1)
                loader.add_value("url", response.url)
                progress_bar.update(1)
                loader.add_xpath('title', '/html/body/div[1]/div/div/div[3]/main/header/h1/span/text()')
                progress_bar.update(1)
                loader.add_xpath('marketing_tags',
                                 '//li[@class="goods-tags__item"]//text()')
                progress_bar.update(1)
                loader.add_xpath('brand', '/html/body/div[1]/div/div/div[3]/main/header/div[2]/div/span[2]/text()')
                progress_bar.update(1)
                loader.add_xpath('section',
                                 '/html/body/div[1]/div/div/div[3]/main/header/div[1]/ul/li/a/span/span/text()')
                progress_bar.update(1)
                loader.add_value('current_price', prices[1])
                progress_bar.update(1)
                loader.add_value('original_price', prices[0])
                progress_bar.update(1)
                loader.add_value('sale_tag',
                                 f'Скидка {self.calculate_sale(prices)}%' if self.is_sale(response) else "Нет скидки")
                progress_bar.update(1)
                loader.add_value('in_stock', self.in_stock_in_pharmacy(response) or self.in_stock_on_site(response))
                progress_bar.update(1)
                loader.add_value('count', 0)
                progress_bar.update(1)
                loader.add_value('main_image', "https://apteka-ot-sklada.ru" + response.xpath(
                    "//div[@class='goods-gallery__active-picture-area "
                    "goods-gallery__active-picture-area_gallery_trigger']//img/@src").extract_first())
                progress_bar.update(1)
                loader.add_xpath('set_images', '//dev[@class="goods-gallery__sidebar"]//img/@scr')
                progress_bar.update(1)
                loader.add_value('view360', [])
                progress_bar.update(1)
                loader.add_value('video', [])
                progress_bar.update(1)
                loader.add_value('description', description)
                progress_bar.update(1)
                loader.add_xpath('country_of_origin',
                                 "/html/body/div[1]/div/div/div[3]/main/header/div[2]/div/span[1]/text()")
                progress_bar.update(1)
                yield loader.load_item()

            except Exception as e:
                # В случае возникновения ошибки, залогируем информацию о ней
                self.logger.error(f"Error processing product {response.url}: {str(e)}")
                # Сохраняем информацию об ошибке в отдельный файл
                with open('error_log.txt', 'a') as f:
                    f.write(f"Product URL: {response.url}\n")
                    f.write(f"Error message: {str(e)}\n")
                    f.write("\n")

    def get_description(self, response):
        description = ''.join(response.xpath("//div[@class='ui-collapsed-content__content']//text()").getall())
        return description


# Запустим spider и сохраним результат в JSON файл
if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    save_path = 'parsed_data'

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    current_time = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"{save_path}/apteka_data_{current_time}.json"
    start_time = time.time()
    process = CrawlerProcess(settings={
        'FEEDS': {
            output_file: {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
                'ensure_ascii': False
            }
        }
    })

    process.crawl(AptekaSpider)
    process.start()

    end_time = time.time()  # Засекаем время после окончания парсера
    elapsed_time = end_time - start_time  # Вычисляем разницу во времени
    print(f"Время выполнения программы: {elapsed_time} секунд")

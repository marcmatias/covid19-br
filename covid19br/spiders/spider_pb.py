import io

import re
import scrapy

import rows

from collections import defaultdict

from covid19br.common.base_spider import BaseCovid19Spider
from covid19br.common.constants import State, ReportQuality
from covid19br.common.models.bulletin_models import StateTotalBulletinModel

REGEXP_CASES = re.compile("([0-9.]+) casos confirmados")
REGEXP_DEATHS = re.compile("totaliza ([0-9.]+) (?:mortes)")

class SpiderPB(BaseCovid19Spider):
    name = State.PB.value
    state = State.PB
    information_delay_in_days = 0
    report_qualities = [
        ReportQuality.ONLY_TOTAL
    ]

    source_1_url = 'https://paraiba.pb.gov.br/diretas/saude/coronavirus/noticias'
    source_2_url = 'https://superset.plataformatarget.com.br/superset/explore_json/?form_data=%7B%22slice_id%22%3A1550%7D&csv=true'

    def pre_init(self):
        self.requested_dates = list(self.requested_dates)
    
    def start_requests(self):
        yield scrapy.Request(url=self.source_1_url, callback=self.parse_html)
        yield scrapy.Request(url=self.source_2_url, callback=self.parse_csv)

    def parse_html(self, response, **kwargs):
        news_per_date = defaultdict(list)
        news_div = response.xpath("//div[@class = 'tileContent']")
        for div in news_div:
            title = div.xpath(".//h2/a//text()").get()
            if self.is_covid_report_news(title):
                date = self.normalizer.extract_numeric_date(title)
                url = div.xpath(".//h2/a/@href").get()
                news_per_date[date].append(url)

        for date in news_per_date:
            if date in self.requested_dates:
                for link in news_per_date[date]:
                    yield scrapy.Request(
                        link, callback=self.parse_html_bulletin_text, cb_kwargs={"date": date}
                    )
    
        if self.start_date < min(news_per_date):
            last_page_number = 0
            last_page_url = response.request.url
            if "b_start" in last_page_url:
                url, _query_params = last_page_url.split("?")
                *_params, last_page_number = _query_params.split("=")
                last_page_number = self.normalizer.ensure_integer(last_page_number)
            next_page_number = last_page_number + 30
            next_page_url = f"{self.source_1_url}?b_start:int={next_page_number}"
            yield scrapy.Request(next_page_url, callback=self.parse_html)

    def parse_html_bulletin_text(self, response, date):
        html = response.text
        cases, *_other_matches = REGEXP_CASES.findall(html) or [None]
        deaths, *_other_matches = REGEXP_DEATHS.findall(html) or [None]
        if cases or deaths:
            bulletin = StateTotalBulletinModel(
                date=date,
                state=self.state,
                deaths=deaths,
                confirmed_cases=cases,
                source=response.request.url,
            )
            self.add_new_bulletin_to_report(bulletin, date)

    def parse_csv(self, response):
        totals_report = rows.import_from_csv(
            io.BytesIO(response.body),
            encoding="utf-8-sig",
            force_types={
                "data": rows.fields.DateField,
                "casosAcumulados": rows.fields.IntegerField,
                "obitosAcumulados": rows.fields.IntegerField,
            },
        )
        for row in totals_report:
            date = row.data
            if date in self.requested_dates:
                bulletin = StateTotalBulletinModel(
                    date=date,
                    state=self.state,
                    deaths= row.obitosacumulados,
                    confirmed_cases=row.casosacumulados,
                    source=response.request.url,
                )
                self.add_new_bulletin_to_report(bulletin, date)

    @staticmethod
    def is_covid_report_news(news_title: str) -> bool:
        clean_title = news_title.lower()
        return "atualização" in clean_title and "covid" in clean_title
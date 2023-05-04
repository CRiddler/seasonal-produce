from calendar import monthrange, month_name
from datetime import date, timedelta
from dataclasses import dataclass
from functools import reduce
from urllib.parse import urlparse
from pathlib import Path
from re import finditer

from bs4 import BeautifulSoup, SoupStrainer
from dateutil.relativedelta import relativedelta
from requests import get
from pandas import read_html, date_range, NA, DataFrame, read_csv, Index

MONTHNAMES = [m.lower() for m in month_name]
YEAR = 1970

@dataclass
class PageCache:
    url: str
    cache_base: Path = None

    @property
    def path(self):
        parsed = urlparse(self.url)
        path = parsed.path.strip('/') if parsed.path else 'index'
        if self.cache_base:
            return self.cache_base / parsed.netloc / f'{path}.html'
        return Path(parsed.netloc, f'{path}.html')

    def download(self, force=False):
        if force is False and self.path.exists():
            return self.path.read_text()

        resp = get(self.url)
        resp.raise_for_status()

        self.path.parent.mkdir(exist_ok=True, parents=True)
        with open(self.path, 'w') as f:
            f.write(resp.text)
        return resp.text


def convert_monthrange(start, stop):
    start = convert_month(start)
    stop = convert_month(stop)
    if stop.day == 1:
        stop = stop + relativedelta(months=1, days=-1)
    return start, stop

def convert_month(month_string):
    frac = 0
    if month_string.startswith('mid-'):
        frac = .5
        month_string = month_string.replace('mid-', '')

    elif month_string.startswith('late-'):
        frac = .8
        month_string = month_string.replace('late-', '').strip()

    month_no = MONTHNAMES.index(month_string)
    _, daysinmonth = monthrange(YEAR, month_no)

    if frac == 0:
        day = 1
    else:
        day = int(daysinmonth * frac)
    return date(YEAR, month_no, day)

def extract_dates(verbose_string):
    verbose_string = (
        verbose_string.replace(',', ' and')
        .replace('late ', 'late-')
        .replace('early ', 'early-')
        .replace('jully', 'july')
        .replace('novemeber', 'november')
        .replace('novemebr', 'november')
        .replace('south dakota', '')
        .replace('south', '')
        .replace('janury', 'january')
        .replace(' – ', ' and ')
        .replace('octobert', 'october')
    )

    dates = []

    for match in finditer(r'([a-z-]+) till ([a-z-]+)', verbose_string):
        dates.append(
            convert_monthrange(*match.groups())
        )
        verbose_string = verbose_string.replace(match.group(0), '')


    if verbose_string.strip():
        for month_str in verbose_string.strip().replace('and', '').split():
            dates.append(convert_month(month_str))

    full_dates = []
    for d in dates:
        if isinstance(d, tuple):
            start, stop = d
        else:
            start = d

            _, daysinmonth = monthrange(start.year, start.month)
            stop = d.replace(day=daysinmonth)
        full_dates.append(date_range(start, stop, freq='D'))
    return reduce(Index.union, full_dates)


here = Path(__file__).parent
base = PageCache('https://www.whenistheseason.com', cache_base=here)
states_df = (
    read_csv(here / 'states.csv')
    .rename(columns=str.lower)
    .assign(
        state=lambda d: d['state'].str.lower(),
        region=lambda d: d['region'].str.lower(),
    )
)

records = []
index_soup = BeautifulSoup(base.download(), features='html.parser')
for elem in index_soup.select('figure > a'):
    pagecache = PageCache(elem['href'], cache_base=here)
    soup = BeautifulSoup(pagecache.download(), features='html.parser')

    produce = (
        urlparse(elem['href']).path
        .replace('-in-season', '')
        .replace('-season', '')
        .split('-')[-1]
        .replace('/', '')
    )

    df = (
        read_html(soup.prettify())[0]
    )

    if produce in {'pineapple', 'apple'}:
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]


    df = df.rename(columns=str.lower).dropna(how='all')

    assert 'state' in df.columns
    assert len(df.columns) == 2
    assert df.columns.str.contains('in season').any()

    all_dates = date_range(
        date(YEAR, 1, 1),
        date(YEAR+1, 1, 1) - timedelta(days=1),
        freq='D'
    )

    new_df = (
        DataFrame(NA, columns=states_df['state'], index=all_dates)
        .rename_axis('state', axis=0)
    )

    for _, state, verbose_string in df.itertuples():
        dates = extract_dates(
            verbose_string.casefold().strip()
        )

        if state.lower() == 'massachussets':
            state = 'Massachusetts'
        elif state.lower() in {'northern california', 'southern california'}:
            state = 'California'
        elif state.lower() in {'north florida', 'south florida'}:
            state = 'Florida'

        new_df.loc[:, state.lower()]
        new_df.loc[dates, state.lower()] = 1

    assert new_df.columns.size == 50

    data_dir = here / 'data'
    data_dir.mkdir(exist_ok=True)
    new_df.to_csv(data_dir / f'{produce}.csv')



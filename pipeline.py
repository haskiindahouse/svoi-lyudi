#!/usr/bin/env python3
"""Граф русской элиты 1800–2020: Wikidata-пайплайн.

Стадии (чекпоинты в data/): resolve → groups → crawl → build → all
  python3 pipeline.py resolve   # сиды: имена → QID (data/seeds_resolved.json)
  python3 pipeline.py groups    # члены тусовок по SPARQL (data/group_members.json)
  python3 pipeline.py crawl     # сущности + семейное замыкание (data/entities.json)
  python3 pipeline.py build     # data/graph.json + dist/index.html
"""
import json, sys, time, math, random, pathlib, collections
import urllib.request, urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent
DATA = ROOT / "data"; DATA.mkdir(exist_ok=True)
DIST = ROOT / "dist"; DIST.mkdir(exist_ok=True)
UA = {"User-Agent": "svoi-lyudi/1.0 (+https://github.com/haskiindahouse/svoi-lyudi) urllib"}
API = "https://www.wikidata.org/w/api.php"
SPARQL = "https://query.wikidata.org/sparql"

# ---------- сиды: (имя для поиска, ориентир года рождения) ----------
PERSON_SEEDS = [
    # Империя: литература, живопись, музыка, купцы-меценаты
    ("Александр Сергеевич Пушкин", 1799), ("Василий Андреевич Жуковский", 1783),
    ("Николай Михайлович Карамзин", 1766), ("Пётр Андреевич Вяземский", 1792),
    ("Николай Васильевич Гоголь", 1809), ("Виссарион Григорьевич Белинский", 1811),
    ("Николай Алексеевич Некрасов", 1821), ("Иван Сергеевич Тургенев", 1818),
    ("Фёдор Михайлович Достоевский", 1821), ("Лев Николаевич Толстой", 1828),
    ("Фёдор Иванович Тютчев", 1803), ("Карл Павлович Брюллов", 1799),
    ("Александр Андреевич Иванов", 1806), ("Василий Иванович Суриков", 1848),
    ("Илья Ефимович Репин", 1844), ("Иван Николаевич Крамской", 1837),
    ("Павел Михайлович Третьяков", 1832), ("Сергей Михайлович Третьяков", 1834),
    ("Савва Иванович Мамонтов", 1841), ("Савва Тимофеевич Морозов", 1862),
    ("Иван Абрамович Морозов", 1871), ("Сергей Иванович Щукин", 1854),
    ("Павел Павлович Рябушинский", 1871), ("Николай Павлович Рябушинский", 1877),
    ("Владимир Васильевич Стасов", 1824), ("Пётр Ильич Чайковский", 1840),
    ("Модест Петрович Мусоргский", 1839), ("Николай Андреевич Римский-Корсаков", 1844),
    ("Милий Алексеевич Балакирев", 1837), ("Михаил Иванович Глинка", 1804),
    ("Антон Григорьевич Рубинштейн", 1829), ("Виктор Михайлович Васнецов", 1848),
    ("Василий Дмитриевич Поленов", 1844), ("Валентин Александрович Серов", 1865),
    ("Михаил Александрович Врубель", 1856), ("Константин Алексеевич Коровин", 1861),
    ("Исаак Ильич Левитан", 1860), ("Николай Дмитриевич Телешов", 1867),
    # Серебряный век
    ("Сергей Павлович Дягилев", 1872), ("Александр Николаевич Бенуа", 1870),
    ("Лев Самойлович Бакст", 1866), ("Фёдор Иванович Шаляпин", 1873),
    ("Максим Горький", 1868), ("Антон Павлович Чехов", 1860),
    ("Иван Алексеевич Бунин", 1870), ("Леонид Николаевич Андреев", 1871),
    ("Александр Иванович Куприн", 1870), ("Александр Александрович Блок", 1880),
    ("Андрей Белый", 1880), ("Вячеслав Иванович Иванов", 1866),
    ("Зинаида Николаевна Гиппиус", 1869), ("Дмитрий Сергеевич Мережковский", 1866),
    ("Анна Андреевна Ахматова", 1889), ("Николай Степанович Гумилёв", 1886),
    ("Марина Ивановна Цветаева", 1892), ("Осип Эмильевич Мандельштам", 1891),
    ("Борис Леонидович Пастернак", 1890), ("Владимир Владимирович Маяковский", 1893),
    ("Лиля Юрьевна Брик", 1891), ("Осип Максимович Брик", 1888),
    ("Давид Давидович Бурлюк", 1882), ("Велимир Хлебников", 1885),
    ("Казимир Северинович Малевич", 1879), ("Василий Васильевич Кандинский", 1866),
    ("Владимир Евграфович Татлин", 1885), ("Наталья Сергеевна Гончарова", 1881),
    ("Михаил Фёдорович Ларионов", 1881), ("Константин Сергеевич Станиславский", 1863),
    ("Владимир Иванович Немирович-Данченко", 1858), ("Всеволод Эмильевич Мейерхольд", 1874),
    ("Игорь Фёдорович Стравинский", 1882), ("Сергей Васильевич Рахманинов", 1873),
    ("Александр Николаевич Скрябин", 1872), ("Михаил Алексеевич Кузмин", 1872),
    ("Николай Александрович Бердяев", 1874), ("Владимир Владимирович Набоков", 1899),
    # Власть империи
    ("Николай I", 1796), ("Александр II", 1818), ("Александр III", 1845),
    ("Николай II", 1868), ("Сергей Юльевич Витте", 1849),
    ("Пётр Аркадьевич Столыпин", 1862), ("Константин Петрович Победоносцев", 1827),
    ("Феликс Феликсович Юсупов", 1887), ("Григорий Ефимович Распутин", 1869),
    # СССР: власть
    ("Владимир Ильич Ленин", 1870), ("Иосиф Виссарионович Сталин", 1878),
    ("Лев Давидович Троцкий", 1879), ("Николай Иванович Бухарин", 1888),
    ("Анатолий Васильевич Луначарский", 1875), ("Лаврентий Павлович Берия", 1899),
    ("Никита Сергеевич Хрущёв", 1894), ("Леонид Ильич Брежнев", 1906),
    ("Алексей Николаевич Косыгин", 1904), ("Михаил Андреевич Суслов", 1902),
    ("Юрий Владимирович Андропов", 1914), ("Андрей Андреевич Громыко", 1909),
    ("Екатерина Алексеевна Фурцева", 1910), ("Анастас Иванович Микоян", 1895),
    ("Климент Ефремович Ворошилов", 1881), ("Вячеслав Михайлович Молотов", 1890),
    # СССР: культура и наука
    ("Сергей Михайлович Эйзенштейн", 1898), ("Дмитрий Дмитриевич Шостакович", 1906),
    ("Сергей Сергеевич Прокофьев", 1891), ("Михаил Афанасьевич Булгаков", 1891),
    ("Корней Иванович Чуковский", 1882), ("Алексей Николаевич Толстой", 1883),
    ("Александр Александрович Фадеев", 1901), ("Константин Михайлович Симонов", 1915),
    ("Валентин Петрович Катаев", 1897), ("Илья Григорьевич Эренбург", 1891),
    ("Александр Трифонович Твардовский", 1910), ("Александр Исаевич Солженицын", 1918),
    ("Андрей Дмитриевич Сахаров", 1921), ("Пётр Леонидович Капица", 1894),
    ("Лев Давидович Ландау", 1908), ("Игорь Васильевич Курчатов", 1903),
    ("Сергей Павлович Королёв", 1907), ("Отто Юльевич Шмидт", 1891),
    ("Викентий Викентьевич Вересаев", 1867),
    ("Сергей Владимирович Михалков", 1913), ("Наталья Петровна Кончаловская", 1903),
    ("Пётр Петрович Кончаловский", 1876), ("Никита Сергеевич Михалков", 1945),
    ("Андрей Сергеевич Кончаловский", 1937), ("Евгений Александрович Евтушенко", 1932),
    ("Андрей Андреевич Вознесенский", 1933), ("Белла Ахатовна Ахмадулина", 1937),
    ("Булат Шалвович Окуджава", 1924), ("Владимир Семёнович Высоцкий", 1938),
    ("Александр Аркадьевич Галич", 1918), ("Юрий Петрович Любимов", 1917),
    ("Олег Николаевич Ефремов", 1927), ("Олег Павлович Табаков", 1935),
    ("Андрей Арсеньевич Тарковский", 1932), ("Арсений Александрович Тарковский", 1907),
    ("Василий Макарович Шукшин", 1929), ("Леонид Иович Гайдай", 1923),
    ("Эльдар Александрович Рязанов", 1927), ("Георгий Николаевич Данелия", 1930),
    ("Сергей Фёдорович Бондарчук", 1920), ("Иннокентий Михайлович Смоктуновский", 1925),
    ("Мстислав Леопольдович Ростропович", 1927), ("Галина Павловна Вишневская", 1926),
    ("Святослав Теофилович Рихтер", 1915), ("Майя Михайловна Плисецкая", 1925),
    ("Родион Константинович Щедрин", 1932), ("Иосиф Александрович Бродский", 1940),
    ("Сергей Донатович Довлатов", 1941), ("Виктор Робертович Цой", 1962),
    ("Борис Борисович Гребенщиков", 1953), ("Сергей Анатольевич Курёхин", 1954),
    ("Алла Борисовна Пугачёва", 1949), ("Иосиф Давыдович Кобзон", 1937),
    ("Зураб Константинович Церетели", 1934), ("Илья Сергеевич Глазунов", 1930),
    # Перестройка и 90-е
    ("Михаил Сергеевич Горбачёв", 1931), ("Раиса Максимовна Горбачёва", 1932),
    ("Александр Николаевич Яковлев", 1923), ("Эдуард Амвросиевич Шеварднадзе", 1928),
    ("Борис Николаевич Ельцин", 1931), ("Наина Иосифовна Ельцина", 1932),
    ("Татьяна Борисовна Юмашева", 1960), ("Валентин Борисович Юмашев", 1957),
    ("Александр Стальевич Волошин", 1956), ("Егор Тимурович Гайдар", 1956),
    ("Анатолий Борисович Чубайс", 1955), ("Виктор Степанович Черномырдин", 1938),
    ("Юрий Михайлович Лужков", 1936), ("Анатолий Александрович Собчак", 1937),
    ("Людмила Борисовна Нарусова", 1951), ("Ксения Анатольевна Собчак", 1981),
    ("Галина Васильевна Старовойтова", 1946), ("Борис Ефимович Немцов", 1959),
    ("Борис Абрамович Березовский", 1946), ("Владимир Александрович Гусинский", 1952),
    ("Михаил Борисович Ходорковский", 1963), ("Александр Павлович Смоленский", 1954),
    ("Пётр Олегович Авен", 1955), ("Михаил Маратович Фридман", 1964),
    ("Владимир Олегович Потанин", 1961), ("Роман Аркадьевич Абрамович", 1966),
    ("Олег Владимирович Дерипаска", 1968), ("Михаил Дмитриевич Прохоров", 1965),
    # Путинская эпоха
    ("Владимир Владимирович Путин", 1952), ("Людмила Александровна Очеретная", 1958),
    ("Дмитрий Анатольевич Медведев", 1965), ("Игорь Иванович Сечин", 1960),
    ("Сергей Борисович Иванов", 1953), ("Николай Платонович Патрушев", 1951),
    ("Александр Васильевич Бортников", 1951), ("Сергей Евгеньевич Нарышкин", 1954),
    ("Юрий Валентинович Ковальчук", 1951), ("Михаил Валентинович Ковальчук", 1946),
    ("Аркадий Романович Ротенберг", 1951), ("Борис Романович Ротенберг", 1957),
    ("Геннадий Николаевич Тимченко", 1952), ("Сергей Павлович Ролдугин", 1951),
    ("Сергей Викторович Чемезов", 1952), ("Владимир Иванович Якунин", 1948),
    ("Андрей Александрович Фурсенко", 1949), ("Сергей Александрович Фурсенко", 1954),
    ("Сергей Кужугетович Шойгу", 1955), ("Сергей Викторович Лавров", 1950),
    ("Владислав Юрьевич Сурков", 1964), ("Вячеслав Викторович Володин", 1964),
    ("Сергей Владиленович Кириенко", 1962), ("Сергей Семёнович Собянин", 1958),
    ("Дмитрий Сергеевич Песков", 1967), ("Маргарита Симоновна Симоньян", 1980),
    ("Константин Львович Эрнст", 1961), ("Олег Борисович Добродеев", 1959),
    ("Валерий Абисалович Гергиев", 1953), ("Денис Леонидович Мацуев", 1975),
    ("Юрий Абрамович Башмет", 1953), ("Фёдор Сергеевич Бондарчук", 1967),
    ("Егор Андреевич Кончаловский", 1966), ("Анна Никитична Михалкова", 1974),
    ("Надежда Никитична Михалкова", 1986), ("Артём Никитич Михалков", 1975),
    ("Степан Никитич Михалков", 1966), ("Патриарх Кирилл", 1946),
    ("Алексий II", 1929), ("Тихон Шевкунов", 1958),
    ("Алишер Бурханович Усманов", 1953), ("Сулейман Абусаидович Керимов", 1966),
    ("Игорь Иванович Шувалов", 1967), ("Герман Оскарович Греф", 1964),
    ("Эльвира Сахипзадовна Набиуллина", 1963), ("Алексей Леонидович Кудрин", 1960),
    ("Рамзан Ахматович Кадыров", 1976), ("Павел Валерьевич Дуров", 1984),
    ("Аркадий Юрьевич Волож", 1964), ("Евгений Валентинович Касперский", 1965),
    ("Юрий Борисович Мильнер", 1961), ("Иван Андреевич Ургант", 1978),
    ("Алина Маратовна Кабаева", 1983), ("Владимир Викторович Виноградов", 1955),
    ("Виталий Борисович Малкин", 1952), ("Николай Терентьевич Шамалов", 1950),
    ("Левон Суренович Кочарян", 1930), ("Валерий Сергеевич Золотухин", 1941),
    ("Алла Сергеевна Демидова", 1936), ("Леонид Алексеевич Филатов", 1946),
    ("Вениамин Борисович Смехов", 1940),
    # 2020-е: кабинет, Кремль, силовики
    ("Михаил Владимирович Мишустин", 1966), ("Андрей Рэмович Белоусов", 1959),
    ("Денис Валентинович Мантуров", 1969), ("Дмитрий Николаевич Патрушев", 1977),
    ("Татьяна Алексеевна Голикова", 1966), ("Александр Валентинович Новак", 1971),
    ("Марат Шакирзянович Хуснуллин", 1966), ("Антон Германович Силуанов", 1963),
    ("Дмитрий Юрьевич Григоренко", 1978), ("Виталий Геннадьевич Савельев", 1954),
    ("Сергей Евгеньевич Цивилёв", 1961), ("Ольга Борисовна Любимова", 1980),
    ("Валентина Ивановна Матвиенко", 1949), ("Антон Эдуардович Вайно", 1972),
    ("Алексей Геннадьевич Дюмин", 1972), ("Максим Станиславович Орешкин", 1982),
    ("Владимир Ростиславович Мединский", 1970), ("Алексей Алексеевич Громов", 1960),
    ("Игорь Викторович Краснов", 1975), ("Александр Иванович Бастрыкин", 1953),
    ("Валерий Васильевич Герасимов", 1955), ("Виктор Васильевич Золотов", 1954),
    ("Михаил Ефимович Фрадков", 1950), ("Сергей Владимирович Суровикин", 1966),
    ("Евгений Викторович Пригожин", 1961),
    # 2020-е: династии
    ("Борис Юрьевич Ковальчук", 1977), ("Пётр Михайлович Фрадков", 1978),
    ("Павел Михайлович Фрадков", 1981), ("Анна Евгеньевна Цивилёва", 1972),
    ("Ксения Сергеевна Шойгу", 1991), ("Игорь Аркадьевич Ротенберг", 1973),
    ("Роман Борисович Ротенберг", 1981), ("Глеб Сергеевич Франк", 1982),
    ("Кирилл Николаевич Шамалов", 1982), ("Катерина Владимировна Тихонова", 1986),
    ("Мария Владимировна Воронцова", 1985), ("Сергей Сергеевич Иванов", 1980),
    # 2020-е: бизнес, медиа, культура
    ("Алексей Борисович Миллер", 1962), ("Вагит Юсуфович Алекперов", 1950),
    ("Владимир Сергеевич Лисин", 1956), ("Алексей Александрович Мордашов", 1965),
    ("Андрей Игоревич Мельниченко", 1972), ("Татьяна Владимировна Ким", 1975),
    ("Владимир Рудольфович Соловьёв", 1963), ("Захар Прилепин", 1975),
    ("Константин Юрьевич Богомолов", 1975), ("Ярослав Юрьевич Дронов", 1991),
    ("Юрий Александрович Борисов", 1992), ("Екатерина Михайловна Мизулина", 1984),
]

# группы: (варианты точного ru-лейбла, ключевые слова для скоринга описания)
GROUP_SEEDS = [
    (["Арзамас", "Арзамасское общество безвестных людей"], "литератур кружок общество"),
    (["Зелёная лампа"], "литератур кружок общество"),
    (["Могучая кучка"], "композитор объединение содружество"),
    (["Передвижники", "Товарищество передвижных художественных выставок"], "художник объединение товарищество"),
    (["Абрамцевский художественный кружок", "Мамонтовский кружок"], "художник кружок объединение"),
    (["Мир искусства"], "объединение художественное группа"),
    (["Союз русских художников"], "объединение художник"),
    (["Бубновый валет"], "художник объединение группа"),
    (["Голубая роза"], "художник объединение группа выставка"),
    (["Ослиный хвост"], "художник группа выставка"),
    (["Бродячая собака", "Бродячая собака (кафе)"], "кафе кабаре подвал арт"),
    (["Цех поэтов"], "поэт объединение группа"),
    (["Гилея"], "футурист группа поэт"),
    (["ЛЕФ", "Левый фронт искусств"], "объединение творческое журнал фронт"),
    (["Серапионовы братья"], "литератур объединение группа"),
    (["ОБЭРИУ"], "поэт группа объединение"),
    (["Российская ассоциация пролетарских писателей", "РАПП"], "писатель ассоциация организация"),
    (["Ассоциация художников революционной России", "АХРР"], "художник ассоциация"),
    (["Общество станковистов"], "художник общество объединение"),
    (["УНОВИС"], "художник группа объединение"),
    (["Лианозовская группа"], "художник поэт группа неофициал"),
    (["Митьки"], "художник группа движение"),
    (["Московский концептуализм"], "движение искусств направление"),
    (["Ленинградский рок-клуб"], "рок клуб музык"),
    (["Озеро", "Озеро (дачный кооператив)"], "кооператив дачный"),
    (["Семибанкирщина"], "банкир олигарх группа"),
    (["Московский Английский клуб", "Английский клуб"], "клуб дворян джентльмен"),
]

FAMILY_P = {"P22": "family", "P25": "family", "P40": "family", "P3373": "family", "P1038": "family"}
PP_PROPS = dict(FAMILY_P, **{"P26": "marriage", "P451": "partner", "P1066": "student", "P737": "influence"})
PG_PROPS = {"P463": "circle", "P135": "circle", "P69": "education", "P108": "work"}
ALL_PROPS = list(PP_PROPS) + list(PG_PROPS) + ["P569", "P570", "P106", "P571", "P576", "P31"]

# ---------- инфраструктура ----------

def http_json(url, params=None, retries=5):
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # ponytail: сеть ночью — ретраи с бэкоффом и едем дальше
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"HTTP fail after {retries}: {url[:140]} :: {last}")


def sparql(query):
    return http_json(SPARQL, {"query": query, "format": "json"})


def save(name, obj):
    (DATA / name).write_text(json.dumps(obj, ensure_ascii=False, indent=1))
    print(f"  -> data/{name}")


def load(name):
    return json.loads((DATA / name).read_text())


def wbgetentities(ids):
    """Полные сущности батчами по 50."""
    out = {}
    ids = [i for i in ids if i]
    for k in range(0, len(ids), 50):
        chunk = ids[k:k + 50]
        r = http_json(API, {
            "action": "wbgetentities", "ids": "|".join(chunk), "format": "json",
            "props": "claims|labels|descriptions|sitelinks", "languages": "ru|en",
            "sitefilter": "ruwiki",
        })
        out.update(r.get("entities", {}))
        time.sleep(0.25)
        if (k // 50) % 10 == 9:
            print(f"    fetched {k + 50}/{len(ids)}")
    return out


# ---------- разбор сущности ----------

def year_of(snak):
    try:
        t = snak["datavalue"]["value"]["time"]  # "+1848-01-24T00:00:00Z"
        return int(t[1:5]) * (1 if t[0] == "+" else -1)
    except Exception:
        return None


def qid_of(snak):
    try:
        return snak["datavalue"]["value"]["id"]
    except Exception:
        return None


def parse_entity(e):
    claims = e.get("claims", {})
    def first_year(p):
        for c in claims.get(p, []):
            y = year_of(c.get("mainsnak", {}))
            if y:
                return y
        return None
    def qids(p):
        out = []
        for c in claims.get(p, []):
            q = qid_of(c.get("mainsnak", {}))
            if q:
                quals = c.get("qualifiers", {})
                y1 = next((year_of(s) for s in quals.get("P580", []) if year_of(s)), None)
                y2 = next((year_of(s) for s in quals.get("P582", []) if year_of(s)), None)
                out.append((q, y1, y2))
        return out
    label = (e.get("labels", {}).get("ru") or e.get("labels", {}).get("en") or {}).get("value", e.get("id"))
    desc = (e.get("descriptions", {}).get("ru") or e.get("descriptions", {}).get("en") or {}).get("value", "")
    ruwiki = e.get("sitelinks", {}).get("ruwiki", {}).get("title")
    return {
        "qid": e["id"], "label": label, "desc": desc, "ruwiki": ruwiki,
        "sitelinks": len(e.get("sitelinks", {})) or (1 if ruwiki else 0),
        "human": any(qid_of(c.get("mainsnak", {})) == "Q5" for c in claims.get("P31", [])),
        "birth": first_year("P569"), "death": first_year("P570"),
        "inception": first_year("P571"), "dissolved": first_year("P576"),
        "occup": [q for q, _, _ in qids("P106")],
        "props": {p: qids(p) for p in list(PP_PROPS) + list(PG_PROPS)},
    }


# ---------- стадия 1: resolve ----------

def stage_resolve():
    print("== resolve: персоны ==")
    cand = {}  # name -> [qid, ...]
    for name, hint in PERSON_SEEDS:
        r = http_json(API, {"action": "wbsearchentities", "search": name, "language": "ru",
                            "format": "json", "limit": 3, "type": "item"})
        cand[name] = [x["id"] for x in r.get("search", [])]
        time.sleep(0.12)
    ents = {k: parse_entity(v) for k, v in wbgetentities(sorted({q for v in cand.values() for q in v})).items()
            if "missing" not in v}

    resolved, rejects = {}, []
    for name, hint in PERSON_SEEDS:
        best = None
        for q in cand.get(name, []):
            p = ents.get(q)
            if not p or not p["human"]:
                continue
            if hint and p["birth"] and abs(p["birth"] - hint) > 12:
                continue
            score = (p["ruwiki"] is not None, p["sitelinks"])
            if best is None or score > best[0]:
                best = (score, q, p)
        if best:
            resolved[name] = {"qid": best[1], "label": best[2]["label"], "birth": best[2]["birth"]}
        else:
            rejects.append(name)

    print("== resolve: группы ==")
    labels = sorted({lab for alts, _ in GROUP_SEEDS for lab in alts})
    values = " ".join(json.dumps(l) + "@ru" for l in labels)
    r = sparql("SELECT ?item ?lab WHERE { VALUES ?lab { %s } ?item rdfs:label ?lab }" % values)
    by_label = collections.defaultdict(list)
    for b in r["results"]["bindings"]:
        by_label[b["lab"]["value"]].append(b["item"]["value"].rsplit("/", 1)[1])
    gents = {k: parse_entity(v) for k, v in wbgetentities(sorted({q for v in by_label.values() for q in v})).items()
             if "missing" not in v}

    groups = {}
    for alts, kw in GROUP_SEEDS:
        kws = kw.split()
        best = None
        for lab in alts:
            for q in by_label.get(lab, []):
                g = gents.get(q)
                if not g or g["human"]:
                    continue
                text = (g["desc"] + " " + g["label"]).lower()
                score = (sum(1 for w in kws if w in text), g["ruwiki"] is not None, g["sitelinks"])
                if score[0] == 0:
                    continue
                if best is None or score > best[0]:
                    best = (score, q, g)
        if best:
            groups[alts[0]] = {"qid": best[1], "label": best[2]["label"], "desc": best[2]["desc"],
                               "inception": best[2]["inception"], "dissolved": best[2]["dissolved"]}
        else:
            rejects.append("GROUP: " + alts[0])

    save("seeds_resolved.json", {"persons": resolved, "groups": groups, "rejects": rejects})
    print(f"persons: {len(resolved)}/{len(PERSON_SEEDS)}, groups: {len(groups)}/{len(GROUP_SEEDS)}, rejects: {len(rejects)}")
    for x in rejects:
        print("  REJECT:", x)


# ---------- стадия 2: члены групп ----------

def stage_groups():
    seeds = load("seeds_resolved.json")
    out = {}
    for name, g in seeds["groups"].items():
        q = g["qid"]
        r = sparql("""SELECT DISTINCT ?p WHERE {
          { ?p wdt:P463 wd:%s } UNION { ?p wdt:P135 wd:%s }
          ?p wdt:P31 wd:Q5 . }""" % (q, q))
        ids = [b["p"]["value"].rsplit("/", 1)[1] for b in r["results"]["bindings"]]
        if len(ids) > 250:  # ponytail: подозрительно жирная группа = мусорный резолв, режем
            print(f"  TRUNC {name}: {len(ids)} -> 250")
            ids = ids[:250]
        out[name] = ids
        print(f"  {name}: {len(ids)}")
        time.sleep(1.0)
    save("group_members.json", out)


# ---------- стадия 3: crawl ----------

def valid_person(p, strict):
    if not p["human"]:
        return False
    b = p["birth"]
    if b and b < 1730:
        return False
    if b is None and (p["death"] or 0) < 1800:
        return False
    if strict:
        return p["ruwiki"] is not None and (b or p["death"])
    return p["ruwiki"] is not None or p["sitelinks"] >= 3


def stage_crawl():
    seeds = load("seeds_resolved.json")
    members = load("group_members.json")
    want = {v["qid"] for v in seeds["persons"].values()}
    want |= {q for ids in members.values() for q in ids}
    print(f"== crawl: ядро {len(want)} персон ==")

    persons, fetched = {}, {}
    def fetch_round(ids, strict):
        new = [q for q in ids if q not in fetched]
        got = wbgetentities(new)
        for q, e in got.items():
            fetched[q] = True
            if "missing" in e:
                continue
            p = parse_entity(e)
            if valid_person(p, strict):
                persons[q] = p

    fetch_round(sorted(want), strict=False)
    print(f"  ядро валидно: {len(persons)}")

    # семейное замыкание + учителя/влияние, глубина 2, для новых — строгий фильтр
    for depth in (1, 2):
        frontier = set()
        for p in persons.values():
            for prop in list(FAMILY_P) + ["P26", "P1066", "P737"]:
                frontier |= {q for q, _, _ in p["props"].get(prop, [])}
        frontier -= set(fetched)
        print(f"== crawl: волна {depth}, кандидатов {len(frontier)} ==")
        fetch_round(sorted(frontier), strict=True)
        print(f"  всего персон: {len(persons)}")

    # организации: цели P463/P135/P69/P108, у которых ≥2 персон графа
    cnt = collections.Counter()
    for p in persons.values():
        for prop in PG_PROPS:
            cnt.update({q for q, _, _ in p["props"].get(prop, [])})
    group_qids = {g["qid"] for g in seeds["groups"].values()}
    org_ids = {q for q, c in cnt.items() if c >= 2 or q in group_qids}
    org_ids -= set(persons)
    print(f"== crawl: организаций {len(org_ids)} ==")
    orgs = {}
    for q, e in wbgetentities(sorted(org_ids)).items():
        if "missing" not in e:
            o = parse_entity(e)
            if not o["human"]:
                orgs[q] = o
    save("entities.json", {"persons": persons, "orgs": orgs})
    print(f"итог: персон {len(persons)}, организаций {len(orgs)}")


# ---------- стадия 4: build ----------

SPHERES = [
    ("власть", "политик государственный деятель дипломат министр президент премьер губернатор мэр депутат монарх император царь партийный революционер чиновник"),
    ("искусство", "художник живописец писатель поэт композитор режиссёр актёр актриса певец певица музыкант скульптор архитектор балет танцовщик дирижёр драматург сценарист фотограф искусствовед литератор журналист телеведущий продюсер критик переводчик издатель галерист коллекционер меценат"),
    ("деньги", "предприниматель бизнесмен банкир купец промышленник миллиардер олигарх экономист финансист менеджер топ-менеджер инвестор"),
    ("силовики", "офицер разведчик чекист сотрудник кгб фсб военный генерал маршал адмирал полковник"),
    ("наука", "физик математик химик биолог учёный инженер конструктор академик историк философ социолог геолог астроном"),
    ("церковь", "священник епископ митрополит патриарх богослов архиерей монах"),
]


def canon_label(lab):
    """«Михалков, Сергей Владимирович» → «Сергей Владимирович Михалков»."""
    if "," in lab:
        fam, _, rest = lab.partition(",")
        if rest.strip():
            lab = rest.strip() + " " + fam.strip()
    return lab


def short_label(lab):
    """«Сергей Владимирович Михалков» → «Сергей Михалков» (если среднее — отчество)."""
    lab = canon_label(lab)
    parts = lab.split()
    if len(parts) == 3 and parts[1].endswith(("ич", "вна", "чна", "оглы", "кызы")):
        return parts[0] + " " + parts[2]
    return lab


def norm_key(lab):
    return short_label(lab).lower().replace("ё", "е")


def sphere_for(occ_labels):
    text = " ".join(occ_labels).lower()
    scores = [(sum(1 for w in kw.split() if w in text), name) for name, kw in SPHERES]
    best = max(scores)
    return best[1] if best[0] > 0 else "прочее"


def alive_span(p):
    b, d = p["birth"], p["death"]
    if b and not d:
        d = min(b + 90, 2026)
    return (b or 1800), (d or 2026)


def fetch_wiki_cards(titles):
    """Интро-выдержки + миниатюры из ruwiki, кэш в data/wiki_cards.json."""
    cards = json.loads((DATA / "wiki_cards.json").read_text()) if (DATA / "wiki_cards.json").exists() else {}
    todo = [t for t in titles if t not in cards]
    for k in range(0, len(todo), 20):
        chunk = todo[k:k + 20]
        r = http_json("https://ru.wikipedia.org/w/api.php", {
            "action": "query", "format": "json", "prop": "extracts|pageimages",
            "exintro": 1, "explaintext": 1, "exchars": 480, "exlimit": "max",
            "piprop": "thumbnail", "pithumbsize": 240, "pilimit": "max",
            "redirects": 1, "titles": "|".join(chunk)})
        qr = r.get("query", {})
        back = {}  # нормализация/редиректы: итоговый title -> исходный
        for m in qr.get("normalized", []) + qr.get("redirects", []):
            back[m["to"]] = back.get(m["from"], m["from"])
        for pg in qr.get("pages", {}).values():
            t = back.get(pg.get("title"), pg.get("title"))
            if t in todo or pg.get("title") in todo:
                key = t if t in todo else pg["title"]
                cards[key] = {"extract": (pg.get("extract") or "").strip(),
                              "img": (pg.get("thumbnail") or {}).get("source")}
        for t in chunk:
            cards.setdefault(t, {"extract": "", "img": None})
        time.sleep(0.3)
        if (k // 20) % 20 == 19:
            print(f"    wiki cards {k + 20}/{len(todo)}")
    if todo:
        save("wiki_cards.json", cards)
    return cards


def stage_build():
    import networkx as nx
    seeds = load("seeds_resolved.json")
    ents = load("entities.json")
    persons, orgs = ents["persons"], ents["orgs"]
    curated = json.loads((ROOT / "curated.json").read_text())

    # лейблы занятий для сфер (кэш — чтобы пересборка не ходила в сеть)
    occ_ids = sorted({q for p in persons.values() for q in p["occup"]})
    occ_labels = json.loads((DATA / "occ_labels.json").read_text()) if (DATA / "occ_labels.json").exists() else {}
    missing = [q for q in occ_ids if q not in occ_labels]
    if missing:
        for q, e in wbgetentities(missing).items():
            if "missing" not in e:
                occ_labels[q] = (e.get("labels", {}).get("ru") or e.get("labels", {}).get("en") or {}).get("value", "")
        save("occ_labels.json", occ_labels)

    group_qids = {g["qid"] for g in seeds["groups"].values()}
    wiki_cards = fetch_wiki_cards(sorted(
        {p["ruwiki"] for p in list(persons.values()) + list(orgs.values()) if p.get("ruwiki")}))
    nodes, links, seen_edge = {}, [], set()

    def wd_url(q):
        return "https://www.wikidata.org/wiki/" + q

    def ru_url(title):
        return "https://ru.wikipedia.org/wiki/" + urllib.parse.quote(title.replace(" ", "_"))

    # коллизии коротких имён (отец/сын-тёзки): короткую форму получает самый известный
    shorts = collections.Counter(short_label(p["label"]) for p in persons.values())
    best_short = {}
    for q, p in persons.items():
        s = short_label(p["label"])
        if s not in best_short or p["sitelinks"] > persons[best_short[s]]["sitelinks"]:
            best_short[s] = q

    for q, p in persons.items():
        s = short_label(p["label"])
        disp = s if shorts[s] == 1 or best_short[s] == q else canon_label(p["label"])
        wc = wiki_cards.get(p["ruwiki"] or "", {})
        nodes[q] = {"id": q, "label": disp, "type": "person", "birth": p["birth"], "death": p["death"],
                    "sphere": sphere_for([occ_labels.get(o, "") for o in p["occup"]]), "desc": p["desc"],
                    "extract": wc.get("extract") or "", "img": wc.get("img"),
                    "url": ru_url(p["ruwiki"]) if p["ruwiki"] else wd_url(q)}

    def add_org_node(q):
        if q in nodes or q not in orgs:
            return q in nodes
        o = orgs[q]
        if o["label"].lower().startswith(("список", "list of")):  # категории-списки ≠ организации
            return False
        wc = wiki_cards.get(o["ruwiki"] or "", {})
        nodes[q] = {"id": q, "label": o["label"], "type": "circle" if q in group_qids else "institution",
                    "birth": o["inception"], "death": o["dissolved"], "sphere": "тусовка" if q in group_qids else "институция",
                    "desc": o["desc"], "extract": wc.get("extract") or "", "img": wc.get("img"),
                    "url": ru_url(o["ruwiki"]) if o["ruwiki"] else wd_url(q)}
        return True

    def add_edge(a, b, etype, y1, y2, src):
        key = (min(a, b), max(a, b), etype)
        if key in seen_edge or a == b:
            return
        seen_edge.add(key)
        links.append({"s": a, "t": b, "type": etype, "from": y1, "to": y2, "src": src})

    for q, p in persons.items():
        a1, a2 = alive_span(p)
        for prop, etype in PP_PROPS.items():
            for tq, y1, y2 in p["props"].get(prop, []):
                if tq not in persons:
                    continue
                t = persons[tq]
                b1, b2 = alive_span(t)
                lo, hi = max(a1, b1), min(a2, b2)
                if etype == "family" and prop in ("P22", "P25"):
                    lo, hi = a1, min(a2, b2)  # родитель: с рождения ребёнка
                if etype == "marriage":
                    lo = y1 or max(lo, max(a1, b1) + 18)
                    hi = y2 or hi
                if etype == "student":
                    lo, hi = y1 or max(a1 + 15, b1), y2 or min(a1 + 40, b2)
                if etype == "influence":
                    lo, hi = a1 + 15, a2
                if lo > hi:
                    lo, hi = min(lo, hi), max(lo, hi)
                add_edge(q, tq, etype, y1 or lo, y2 or hi, wd_url(q))
        for prop, etype in PG_PROPS.items():
            for tq, y1, y2 in p["props"].get(prop, []):
                if tq in persons or not add_org_node(tq):
                    continue
                o = orgs[tq]
                lo = y1 or max(a1 + 15, o["inception"] or a1)
                hi = y2 or min(a2, o["dissolved"] or 2026, 2026)
                if lo > hi:
                    lo, hi = min(lo, hi), max(lo, hi)
                add_edge(q, tq, "circle" if tq in group_qids else etype, lo, hi, wd_url(q))

    # кураторский слой
    label_idx = {}
    for q, n in nodes.items():
        label_idx.setdefault(norm_key(n["label"]), q)
        parts = norm_key(n["label"]).split()
        if len(parts) >= 3:  # «имя отчество фамилия» → ещё и «имя фамилия»
            label_idx.setdefault(parts[0] + " " + parts[-1], q)
    unmatched = []
    for g in curated["groups"]:
        gid = "C:" + g["name"]
        if g.get("wikidata_label") and norm_key(g["wikidata_label"]) in label_idx:
            gid = label_idx[norm_key(g["wikidata_label"])]
            # у Wikidata часто нет дат жизни группы — добиваем кураторскими
            nodes[gid]["birth"] = nodes[gid]["birth"] or g.get("from")
            nodes[gid]["death"] = nodes[gid]["death"] or g.get("to")
        else:
            nodes[gid] = {"id": gid, "label": g["name"], "type": "circle", "birth": g.get("from"),
                          "death": g.get("to"), "sphere": "тусовка", "desc": g.get("desc", ""), "url": g["src"]}
        for m in g["members"]:
            mq = label_idx.get(norm_key(m))
            if not mq:
                unmatched.append(f"{g['name']}: {m}")
                continue
            p = persons.get(mq)
            a1, a2 = alive_span(p) if p else (g.get("from"), g.get("to"))
            lo = max(g.get("from") or a1, a1)
            hi = min(g.get("to") or a2, a2)
            add_edge(mq, gid, "circle", lo, hi, g["src"])
    for e in curated.get("edges", []):
        a, b = label_idx.get(norm_key(e["a"])), label_idx.get(norm_key(e["b"]))
        if not a or not b:
            unmatched.append(f"edge: {e['a']} -- {e['b']}")
            continue
        add_edge(a, b, e["type"], e.get("from"), e.get("to"), e["src"])
    if unmatched:
        print("КУРАТОРСКИЕ НЕ НАШЛИСЬ:")
        for u in unmatched:
            print("  ", u)

    # выкидываем изолятов и раздутые хабы-институции
    deg = collections.Counter()
    for l in links:
        deg[l["s"]] += 1; deg[l["t"]] += 1
    drop = {q for q, n in nodes.items()
            if (deg[q] == 0) or (n["type"] == "institution" and deg[q] > 200)}
    nodes = {q: n for q, n in nodes.items() if q not in drop}
    links = [l for l in links if l["s"] in nodes and l["t"] in nodes]
    deg = collections.Counter()
    for l in links:
        deg[l["s"]] += 1; deg[l["t"]] += 1
    nodes = {q: n for q, n in nodes.items() if deg[q] > 0}
    links = [l for l in links if l["s"] in nodes and l["t"] in nodes]

    # граф, сообщества, центральность, раскладки
    G = nx.Graph()
    G.add_nodes_from(nodes)
    G.add_edges_from((l["s"], l["t"]) for l in links)
    comms = nx.community.louvain_communities(G, seed=1)
    for i, c in enumerate(sorted(comms, key=len, reverse=True)):
        for q in c:
            nodes[q]["community"] = i
    btw = nx.betweenness_centrality(G, k=min(300, len(G)), seed=1)
    for q, v in btw.items():
        nodes[q]["btw"] = round(v, 5)

    print("  spring layout...")
    pos2 = nx.spring_layout(G, k=1.2 / math.sqrt(len(G)), iterations=120, seed=2)

    # хронологическая раскладка: x = год (рождение+15 для персон), y = барицентр соседей
    def anchor_year(n):
        if n["type"] == "person":
            return (n["birth"] or 1830) + 25
        ys = [nodes[o]["birth"] or 1830 for o in G[n["id"]] if nodes[o]["type"] == "person"]
        return (n["birth"] or (sum(ys) / len(ys) if ys else 1900)) + (10 if n["birth"] else 25)
    rng = random.Random(7)
    ax = {q: min(max(anchor_year(n), 1780), 2026) for q, n in nodes.items()}

    # дорожки по сферам: y-полосы шириной ~sqrt(численности), внутри — барицентр
    lane_order = ["власть", "силовики", "деньги", "церковь", "наука", "прочее", "искусство"]
    cnt_sph = collections.Counter(n["sphere"] for n in nodes.values() if n["type"] == "person")
    ws = [max(0.05, math.sqrt(cnt_sph.get(s, 1))) for s in lane_order]
    ws = [w / sum(ws) for w in ws]
    lane_lo, acc = {}, 0.0
    for s, w in zip(lane_order, ws):
        lane_lo[s] = (acc + 0.008, acc + w - 0.008)
        acc += w

    ay = {}
    for q, n in nodes.items():
        lo, hi = lane_lo.get(n["sphere"], (0.0, 1.0))
        ay[q] = rng.uniform(lo, hi)
    for it in range(60):
        for q, n in nodes.items():
            nb = list(G[q])
            if not nb:
                continue
            m = sum(ay[o] for o in nb) / len(nb)
            if n["type"] == "person":
                lo, hi = lane_lo[n["sphere"]]
                c = (lo + hi) / 2
                ay[q] = min(hi, max(lo, 0.45 * ay[q] + 0.35 * m + 0.2 * c))
            else:  # тусовки и институции плавают к своим людям
                ay[q] = 0.5 * ay[q] + 0.5 * m
    # разгон наложений: внутри корзин (5 лет × дорожка), оргов не трогаем
    bins = collections.defaultdict(list)
    for q, n in nodes.items():
        if n["type"] == "person":
            bins[(int(ax[q]) // 5, n["sphere"])].append(q)
    for (_, s), b in bins.items():
        b.sort(key=lambda q: ay[q])
        lo, hi = lane_lo[s]
        for i, q in enumerate(b):
            ay[q] = 0.4 * ay[q] + 0.6 * (lo + (hi - lo) * (i + 0.5) / len(b))

    for q, n in nodes.items():
        n["x1"] = round((ax[q] - 1780) / 246, 4)
        n["y1"] = round(ay[q], 4)
        n["x2"] = round(float(pos2[q][0]) / 2 + 0.5, 4)
        n["y2"] = round(float(pos2[q][1]) / 2 + 0.5, 4)
        n["deg"] = deg[q]

    graph = {"meta": {"built": "pipeline", "nodes": len(nodes), "links": len(links),
                      "lanes": [{"name": s, "lo": round(lane_lo[s][0], 3), "hi": round(lane_lo[s][1], 3)}
                                for s in lane_order]},
             "nodes": list(nodes.values()), "links": links}
    save("graph.json", graph)

    tpl = (ROOT / "template.html").read_text()
    data_js = "window.GRAPH=" + json.dumps(graph, ensure_ascii=False).replace("</", "<\\/") + ";"
    body = tpl.replace("/*__DATA__*/", data_js)
    (DIST / "artifact.html").write_text(
        "<title>Свои люди — граф русской элиты 1800–2026</title>\n" + body)  # body-only: Artifact сам оборачивает
    full = ('<!doctype html><html lang="ru"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            "<title>Свои люди — граф русской элиты 1800–2026</title></head><body>"
            + body + "</body></html>")
    (DIST / "index.html").write_text(full)
    if (ROOT / "docs").is_dir():  # копия для GitHub Pages
        (ROOT / "docs" / "index.html").write_text(full)
    print(f"dist/index.html: узлов {len(nodes)}, рёбер {len(links)}")
    # ponytail: проверка-минимум — граф не пустой и главная цепочка на месте
    labs = {n["label"] for n in nodes.values()}
    for need in ("Владимир Путин", "Никита Михалков", "Василий Суриков"):
        assert any(need in l for l in labs), f"нет узла: {need}"
    assert len(links) > 1000, "подозрительно мало рёбер"


STAGES = {"resolve": stage_resolve, "groups": stage_groups, "crawl": stage_crawl, "build": stage_build}

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    for name in (STAGES if arg == "all" else {arg: STAGES[arg]}):
        t = time.time()
        print(f"==== {name} ====")
        STAGES[name]()
        print(f"==== {name} done in {time.time() - t:.0f}s ====")

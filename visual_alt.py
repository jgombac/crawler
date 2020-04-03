from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure


class Domain:
    def __init__(self, id, url, total_to):
        self.id = id
        self.url = url
        self.total_links = total_to
        self.links = dict()

    def __repr__(self):
        return f"{self.id}: {self.url} - Linked: {self.total_links}"


CONNECTION_STRING = "postgres://postgres:postgres@192.168.99.100:5432/crawldb"
ENGINE = create_engine(CONNECTION_STRING, echo=False)
Session = scoped_session(sessionmaker(bind=ENGINE))
db = Session()
db.execute("SET search_path TO crawldb")
limit = 9

sites = db.execute("SELECT id, domain FROM site")

domains = [(e[0], e[1]) for e in sites]

domain_info_to = []
domain_info_from = []

for domain in domains:
    count = db.execute(f"SELECT COUNT(*) "
                       f"FROM page fp "
                       f"INNER JOIN link lk ON fp.id = lk.from_page "
                       f"INNER JOIN page tp ON lk.to_page = tp.id "
                       f"WHERE tp.site_id = {domain[0]} and fp.site_id != {domain[0]}")
    info = Domain(domain[0], domain[1], count.first()[0])
    domain_info_to.append(info)
    print(info)

    count = db.execute(f"SELECT COUNT(*) "
                       f"FROM page fp "
                       f"INNER JOIN link lk ON fp.id = lk.from_page "
                       f"INNER JOIN page tp ON lk.to_page = tp.id "
                       f"WHERE tp.site_id != {domain[0]} and fp.site_id = {domain[0]}")
    info = Domain(domain[0], domain[1], count.first()[0])
    domain_info_from.append(info)
    print(info)

domain_info_to.sort(key=lambda x: x.total_links, reverse=True)
domain_info_to = domain_info_to[:limit]

domain_info_from.sort(key=lambda x: x.total_links, reverse=True)
domain_info_from = domain_info_from[:limit]

print(domain_info_to)
print(domain_info_from)

for to_domain in domain_info_to:
    total = 0
    for from_domain in domain_info_from:
        if from_domain.id != to_domain.id:
            count = db.execute(f"SELECT COUNT(*) "
                               f"FROM crawldb.page fp "
                               f"INNER JOIN crawldb.link lk ON fp.id = lk.from_page "
                               f"INNER JOIN crawldb.page tp ON lk.to_page = tp.id "
                               f"WHERE tp.site_id = {to_domain.id} and fp.site_id = {from_domain.id}")
            nr = count.first()[0]
        else:
            nr = 0
        to_domain.links[from_domain.id] = nr
        total += nr
    to_domain.links[-1] = to_domain.total_links-total
    print(f"Done: {to_domain.url}")

print(domain_info_to)

figure(num=None, figsize=(10, 8), dpi=300, facecolor='w', edgecolor='k')
N = len(domain_info_to)
ind = np.arange(N)
width = 0.35

legend_names = [e.url for e in domain_info_from]
legend_names.append("other")
legend_other = []

height = np.zeros(len(domain_info_to))
other = [e.links[-1] for e in domain_info_to]
po = plt.bar(ind, other, bottom=height)
height += other
for domain in domain_info_from:
    data = [e.links[domain.id] for e in domain_info_to]
    pd = plt.bar(ind, data, bottom=height)
    legend_other.append(pd[0])
    height += data
legend_other.append(po[0])

plt.ylabel('Inbound Links')
# plt.title('Domains by Number of Inbound Links')
plt.xticks(ind, [e.url for e in domain_info_to], rotation=80)
plt.yticks(np.arange(0, 17000, 1000))
plt.legend(tuple(legend_other), tuple(legend_names), title="Link Source")
plt.show()

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure


class Domain:

    def __init__(self, id, url, total_to):
        self.id = id
        self.url = url
        self.total_to = total_to
        self.to = dict()

    def __repr__(self):
        return f"{self.id}: {self.url} - Linked from: {self.total_to}"


CONNECTION_STRING = "postgres://postgres:postgres@192.168.99.100:5432/crawldb"
ENGINE = create_engine(CONNECTION_STRING, echo=False)
Session = scoped_session(sessionmaker(bind=ENGINE))
db = Session()
db.execute("SET search_path TO crawldb")
limit = 9

sites = db.execute("SELECT id, domain FROM site")

domains = [(e[0], e[1]) for e in sites]

domain_info = []

for domain in domains:
    count = db.execute(f"SELECT COUNT(*) FROM page tp "
                       f"JOIN link lk on tp.id = lk.to_page "
                       f"WHERE tp.site_id = {domain[0]}")
    info = Domain(domain[0], domain[1], count.first()[0])
    domain_info.append(info)
    print(info)

domain_info.sort(key=lambda x: x.total_to, reverse=True)
domain_info = domain_info[:limit]
print(domain_info)

for to_domain in domain_info:
    total = 0
    for from_domain in domain_info:
        count = db.execute(f"SELECT COUNT(*) "
                           f"FROM crawldb.page fp "
                           f"INNER JOIN crawldb.link lk ON fp.id = lk.from_page "
                           f"INNER JOIN crawldb.page tp ON lk.to_page = tp.id "
                           f"WHERE tp.site_id = {to_domain.id} and fp.site_id = {from_domain.id}")
        nr = count.first()[0]
        to_domain.to[from_domain.id] = nr
        total += nr
    to_domain.to[-1] = to_domain.total_to-total
    print(f"Done: {to_domain.url}")

print(domain_info)

figure(num=None, figsize=(10, 8), dpi=300, facecolor='w', edgecolor='k')
N = len(domain_info)
ind = np.arange(N)
width = 0.35

legend_names = [e.url for e in domain_info]
legend_names.append("other")
legend_other = []

height = np.zeros(len(domain_info))
other = [e.to[-1] for e in domain_info]
po = plt.bar(ind, other, bottom=height)
height += other
for domain in domain_info:
    data = [e.to[domain.id] for e in domain_info]
    pd = plt.bar(ind, data, bottom=height)
    legend_other.append(pd[0])
    height += data
legend_other.append(po[0])

plt.ylabel('Inbound links')
plt.title('Domains by Number of Inbound Links')
plt.xticks(ind, [e.url for e in domain_info], rotation=80)
plt.yticks(np.arange(0, 180000, 10000))
plt.legend(tuple(legend_other), tuple(legend_names), title="Link Source")
plt.show()

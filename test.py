from fs import DisList, DisTable

# declare a DisTable
test = DisTable({'test':'test'}, tableName='TEST')

# retrieve the DisTable
jobTable = DisTable(tableName='TEST')

jobTable['hello'] = {}
print test['hello'].fetch_all()

jobTable['hello']['hi'] = []
jobTable['hello']['hi'].append(1)
jobTable['hello']['hi'].append(2)

fetched = test['hello']['hi'].fetch_all()

for i in fetched:
	print i

print 'hihi'
import happybase
connection = happybase.Connection('localhost')
connection.open()
print connection.tables()

connection.create_table(
    'recommendation',
    {'cf1': dict() }
)
table = connection.table('recommendation')
table.put('userid', {'cf1:rec1': 'value1'})
row = table.row('userid')
print row['cf1:rec1']
        
for key, data in table.scan():
  print key, data

connection.close()

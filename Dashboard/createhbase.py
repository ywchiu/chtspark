import happybase
connection = happybase.Connection('localhost', autoconnect=False)

connection.open()
connection.create_table(
    'mytable',
    { 'cf1': dict(),
    }
)

connection.close()

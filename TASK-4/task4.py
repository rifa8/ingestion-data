from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, insert, text

def create_source_engine():
    protocol = 'postgresql'
    username = 'postgres'
    password = 'pass'
    host = '192.168.0.17'
    port = '5432'
    database = 'store'

    srcEngine = create_engine(f'{protocol}://{username}:{password}@{host}:{port}/{database}')
    return srcEngine

def create_destination_engine():
    protocol = 'postgresql'
    username = 'postgres'
    password = 'pass'
    host = '192.168.0.17'
    port = '15432'
    database = 'store'

    destEngine = create_engine(f'{protocol}://{username}:{password}@{host}:{port}/{database}')
    return destEngine

srcEngine = create_source_engine()
destEngine = create_destination_engine()

srcMeta = MetaData()
srcMeta.reflect(bind=srcEngine)
srcConn = srcEngine.connect()

destMeta = MetaData()
destMeta.reflect(bind=destEngine)
destConn = destEngine.connect()

tables_to_ingest = ['brands', 'products', 'orders', 'order_details']

def create_destination_table(tables_to_ingest, srcConn, destConn, destEngine):
    for table in tables_to_ingest:
        srcTable = Table(f'{table}', srcMeta)
        destTable = Table(f'{table}', destMeta)

        for column in srcTable.columns:
            new_column = Column(column.name, column.type)
            
            if column.primary_key:
                new_column.primary_key = True
            
            if column.foreign_keys:
                foreign_key_column = list(column.foreign_keys)[0]
                referenced_table = foreign_key_column.column.table.name
                referenced_column = foreign_key_column.column.name
                new_column.append_foreign_key(ForeignKey(f'{referenced_table}.{referenced_column}'))
            
            if not column.nullable:
                new_column.nullable = False
            
            destTable.append_column(new_column)

        destTable.create(destEngine)
    #corner case
    sql = text("ALTER TABLE orders ALTER COLUMN order_date SET DEFAULT CURRENT_TIMESTAMP;")
    destConn.execute(sql)
    

def insert_into_destination_table(tables_to_ingest, srcConn, destConn):
    for table in tables_to_ingest:
        srcTable = Table(f'{table}', srcMeta)
        destTable = Table(f'{table}', destMeta)

        select = srcTable.select()
        data = srcConn.execute(select)

        for row in data:
            ins = insert(destTable).values(row)
            destConn.execute(ins)
            destConn.commit()

create_destination_table(tables_to_ingest, srcConn, destConn, destEngine)
insert_into_destination_table(tables_to_ingest, srcConn, destConn)


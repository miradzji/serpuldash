#from jupyter_dash import JupyterDash
from dash import Dash,dcc,html,dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input,Output
from dash.exceptions import PreventUpdate
from dash_bootstrap_templates import load_figure_template
from plotly.subplots import make_subplots
from dash import dcc
import plotly.graph_objects as go


import plotly.express as px
import pandas as pd
import numpy as np

import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from dateutil.relativedelta import relativedelta

spark = SparkSession.builder \
    .appName("Visualisasi Serpul") \
    .master("local") \
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:2.4.1") \
    .config("spark.driver.extraClassPath", "mssql-jdbc-6.1.0.jre8.jar") \
    .config("spark.executor.extraClassPath", "mssql-jdbc-6.1.0.jre8.jar") \
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
    .config("spark.sql.inMemoryColumnarStorage.batchSize",10000) \
    .config("spark.executor.memory", "5g") \
    .config("spark.executor.memory-recomended", "20g") \
    .config("spark.driver.memory", "5g") \
    .config("spark.driver.memory-recomended", "13.95g") \
    .config("spark.cores.max", "7") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

## DATE RANGE ##
today = datetime.today()

sample_date = datetime(today.year, today.month, today.day)
first_day = sample_date + relativedelta(day=1)
last_day = sample_date + relativedelta(day=31)
active_day = sample_date + relativedelta(day=90)

## DATA AKSES ##
dfTransaksiSuksesPra = spark.read.csv("tbl_transaksi_pra_sukses.csv", header=True)
dfTransaksiSuksesPasca = spark.read.csv("tbl_transaksi_pasca_sukses.csv", header=True)
dfTransaksiSuksesLainnya = spark.read.csv("tbl_transaksi_lainnya_sukses.csv", header=True)
dfTransaksiRefundPra = spark.read.csv("tbl_transaksi_pra_refund.csv", header=True)
#dfTransaksiRefundPasca = spark.read.csv("tbl_transaksi_pasca_refund.csv", header=True)
dfLBCustomer = spark.read.csv("tbl_lb_customer.csv", header=True)
dfTransaksiRefundLainnya = spark.read.csv("tbl_transaksi_lainnya_refund.csv", header=True)

# mongo_ip = "mongodb://192.168.1.125:27017/"
# dfTransaksiSuksesPra = spark.read.format("mongo").option("uri",mongo_ip).option("database","serpul_db").option("collection","tbl_transaksi_pra_sukses").load().drop('_id')
# dfTransaksiSuksesPasca = spark.read.format("mongo").option("uri",mongo_ip).option("database","serpul_db").option("collection","tbl_transaksi_pasca_sukses").load().drop('_id')
# dfTransaksiSuksesLainnya = spark.read.format("mongo").option("uri",mongo_ip).option("database","serpul_db").option("collection","tbl_transaksi_lainnya_sukses").load().drop('_id')
# dfTransaksiRefundLainnya = spark.read.format("mongo").option("uri",mongo_ip).option("database","serpul_db").option("collection","tbl_transaksi_lainnya_refund").load().drop('_id')
# dfLBCustomer = spark.read.format("mongo").option("uri",mongo_ip).option("database","serpul_db").option("collection","tbl_lb_customer").load().drop('_id')

dfTransaksiSuksesPra2 = dfTransaksiSuksesPra.groupBy(['tgl_transaksi','customer_id','kategori_produk','jenis_produk','status','vendor']).agg(F.sum(F.col('harga_beli')),F.sum(F.col('harga_jual')),F.sum('laba'),F.count('tgl_transaksi'))
dfTransaksiSuksesPra2 = dfTransaksiSuksesPra2.withColumnRenamed('sum(harga_beli)','harga_beli').withColumnRenamed('sum(harga_jual)','harga_jual').withColumnRenamed('sum(laba)','laba').withColumnRenamed('count(tgl_transaksi)','jumlah_transaksi')
dfTransaksiSuksesPra2 = dfTransaksiSuksesPra2.withColumn('keterangan',F.lit('Sukses'))
dfTransaksiSuksesPra2 = dfTransaksiSuksesPra2.withColumn('tgl_transaksi', F.date_format(F.to_date(F.col('tgl_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))

dfTransaksiSuksesPasca2 = dfTransaksiSuksesPasca.groupBy(['tanggal_transaksi','customer_id','product_name','product_category','status_transaksi']).agg(F.sum(F.col('total_transaksi')),F.sum(F.col('margin')),F.count('tanggal_transaksi'))
dfTransaksiSuksesPasca2 = dfTransaksiSuksesPasca2.withColumnRenamed('sum(total_transaksi)','total_transaksi').withColumnRenamed('sum(margin)','margin').withColumnRenamed('count(tanggal_transaksi)','jumlah_transaksi')
dfTransaksiSuksesPasca2 = dfTransaksiSuksesPasca2.withColumn('tanggal_transaksi', F.date_format(F.to_date(F.col('tanggal_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))

dfTransaksiSuksesLainnya = dfTransaksiSuksesLainnya.withColumn('keterangan',F.split(F.col('description'),' ').getItem(0))
dfTransaksiSuksesLainnya2 = dfTransaksiSuksesLainnya.groupBy(['tgl_transaksi','customer_id','kategori_transaksi','keterangan']).agg(F.sum(F.col('nominal')),F.count('tgl_transaksi'))
dfTransaksiSuksesLainnya2 = dfTransaksiSuksesLainnya2.withColumnRenamed('sum(nominal)','nominal').withColumnRenamed('count(tgl_transaksi)','jumlah_transaksi')
dfTransaksiSuksesLainnya2 = dfTransaksiSuksesLainnya2.withColumn('keterangan',F.lit('Transaksi Lainnya'))
dfTransaksiSuksesLainnya2 = dfTransaksiSuksesLainnya2.withColumn('tgl_transaksi', F.date_format(F.to_date(F.col('tgl_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))

# dfTransaksiRefundPra2 = dfTransaksiRefundPra.groupBy(['tgl_transaksi','kategori_produk','jenis_produk','status','vendor']).agg(F.sum(F.col('harga_beli')),F.sum(F.col('harga_jual')),F.sum('laba'),F.count('tgl_transaksi'))
# dfTransaksiRefundPra2 = dfTransaksiRefundPra2.withColumnRenamed('sum(harga_beli)','harga_beli').withColumnRenamed('sum(harga_jual)','harga_jual').withColumnRenamed('sum(laba)','laba').withColumnRenamed('count(tgl_transaksi)','jumlah_transaksi')
# dfTransaksiRefundPra2 = dfTransaksiRefundPra2.withColumn('keterangan',F.lit('Gagal'))
# dfTransaksiRefundPra2 = dfTransaksiRefundPra2.withColumn('tgl_transaksi', F.date_format(F.to_date(F.col('tgl_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))

dfTransaksiRefundLainnya = dfTransaksiRefundLainnya.withColumn('keterangan',F.split(F.col('description'),' ').getItem(0))
dfTransaksiRefundLainnya2 = dfTransaksiRefundLainnya.groupBy(['tgl_transaksi','customer_id','kategori_transaksi','keterangan']).agg(F.sum(F.col('nominal')),F.count('tgl_transaksi'))
dfTransaksiRefundLainnya2 = dfTransaksiRefundLainnya2.withColumnRenamed('sum(nominal)','nominal').withColumnRenamed('count(tgl_transaksi)','jumlah_transaksi')
dfTransaksiRefundLainnya2 = dfTransaksiRefundLainnya2.withColumn('keterangan',F.lit('Refund Lainnya'))
dfTransaksiRefundLainnya2 = dfTransaksiRefundLainnya2.withColumn('tgl_transaksi', F.date_format(F.to_date(F.col('tgl_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))

dfLBCustomer2 = dfLBCustomer.withColumn('tgl_transaksi', F.date_format(F.to_date(F.col('tgl_transaksi'),'dd/MM/yyyy'),'yyyy-MM-dd'))
dfLBCustomerAktif = dfLBCustomer2.where(F.col('tgl_transaksi').between(first_day,active_day)).groupBy(['customer_id','paket','status']).agg(F.count('tgl_transaksi'),F.sum('deposit'),F.sum('refund'),F.sum('nilai_mutasi_transaksi'))
dfLBCustomerAktif = dfLBCustomerAktif.withColumnRenamed('sum(deposit)','deposit').withColumnRenamed('sum(refund)','refund').withColumnRenamed('sum(nilai_mutasi_transaksi)','nilai_mutasi_transaksi')

dfLBCustomerAktif2 = dfLBCustomerAktif.withColumn('status_tenant', F.when((dfLBCustomerAktif.deposit.cast('int') > 0) | (dfLBCustomerAktif.refund.cast('int') > 0) | (dfLBCustomerAktif.nilai_mutasi_transaksi.cast('int') > 0),'Active Transaction').otherwise('Inactive Transaction'))

dfTransaksiSuksesPraSum = dfTransaksiSuksesPra2.select(
    F.col("tgl_transaksi"),
    F.col("customer_id"),
    F.col("kategori_produk"),
    F.col("status"),
    F.col("harga_beli"),
    F.col("harga_jual"),
    F.col("laba"),
    F.col("jumlah_transaksi")
).withColumn("transaksi", F.lit("PRA"))

dfTransaksiSuksesPascaSum = dfTransaksiSuksesPasca2.select(
    F.col("tanggal_transaksi").alias('tgl_transaksi'),
    F.col("customer_id"),
    F.col("product_category").alias("kategori_produk"),
    F.col("status_transaksi").alias("status"),
    F.col("total_transaksi").alias("harga_jual"),
    F.col("margin").alias("laba"),
    F.col("jumlah_transaksi")
).withColumn("harga_beli",(F.col("harga_jual").cast("int"))-(F.col("laba").cast("int")))

dfTransaksiSuksesPascaSum = dfTransaksiSuksesPascaSum.select(
    F.col("tgl_transaksi"),
    F.col("customer_id"),
    F.col("kategori_produk"),
    F.col("status"),
    F.col("harga_beli"),
    F.col("harga_jual"),
    F.col("laba"),
    F.col("jumlah_transaksi")
).withColumn("status", F.when(F.col("status")=="PAID","SUCCESS").otherwise(F.when(F.col("status")=="PENDING","HOLDON").otherwise(F.col("status")))).withColumn("transaksi", F.lit("PASCA"))

dfTransaksiSuksesLainnyaSum = dfTransaksiSuksesLainnya2.select(
    F.col("tgl_transaksi"),
    F.col("customer_id"),
    F.col("kategori_transaksi").alias("kategori_produk"),
    F.col("nominal").alias("harga_jual"),
    F.col("jumlah_transaksi")
).withColumn("status",F.lit("SUCCESS")).withColumn("harga_beli", F.lit(0)).withColumn("laba", F.col("harga_jual")).withColumn("transaksi", F.lit("LAINNYA"))

dfTransaksiSuksesRefLainnyaSum = dfTransaksiRefundLainnya2.select(
    F.col("tgl_transaksi"),
    F.col("customer_id"),
    F.col("kategori_transaksi").alias("kategori_produk"),
    F.col("nominal").alias("harga_jual"),
    F.col("jumlah_transaksi")
).withColumn("status",F.lit("SUCCESS")).withColumn("harga_beli", F.lit(0)).withColumn("laba", F.col("harga_jual")).withColumn("transaksi", F.lit("DEP/REF"))

my_format = [row.asDict(recursive=True) for row in dfTransaksiSuksesPraSum.collect()]
dfTransaksiSuksesPra2 = pd.DataFrame.from_dict(my_format)

my_format = [row.asDict(recursive=True) for row in dfTransaksiSuksesPascaSum.collect()]
dfTransaksiSuksesPasca2 = pd.DataFrame.from_dict(my_format)

my_format = [row.asDict(recursive=True) for row in dfTransaksiSuksesLainnyaSum.collect()]
dfTransaksiSuksesLainnya2 = pd.DataFrame.from_dict(my_format)

# my_format = [row.asDict(recursive=True) for row in dfTransaksiRefundPra2.collect()]
# dfTransaksiRefundPra2 = pd.DataFrame.from_dict(my_format)

# my_format = [row.asDict(recursive=True) for row in dfTransaksiRefundPra2.collect()]
# dfTransaksiRefundPra = pd.DataFrame.from_dict(my_format)

my_format = [row.asDict(recursive=True) for row in dfTransaksiSuksesRefLainnyaSum.collect()]
dfTransaksiRefundLainnya2 = pd.DataFrame.from_dict(my_format)

my_format = [row.asDict(recursive=True) for row in dfLBCustomer2.collect()]
dfLBCustomer2 = pd.DataFrame.from_dict(my_format)

my_format = [row.asDict(recursive=True) for row in dfLBCustomerAktif2.collect()]
dfLBCustomerAktif2 = pd.DataFrame.from_dict(my_format)

def buat_table(table):
    return dash_table.DataTable(
        table.to_dict('records'), 
        #table,
        [{"name": i, "id": i} for i in table.columns],
        fixed_rows={'headers': True},
        style_table={
            "minHeight":"80vh",
            "height":"80vh",
            "overflowY":"scroll"
        },
        style_cell={
            "whitespace":"normal",
            "height":"auto",
            "fontFamily":"verdana"

        },
        style_data={
            "fontSize":12
        },
        style_data_conditional=[{
            "if":{"column_id":"tgl_transaksi"},
            "width":"120px",
            "textAlign":"left",
            "textDecoration":"underline",
            "cursor":"pointer"
        },
        {"if" : {"row_index":"odd"}, "backgoundColor":"#fafbfb"}
        ],
    )

    
def buat_tab(content, label,value):
    return dcc.Tab(
        content,
        label=label,
        value=value,
        id=f"{value}-tab",
        className="single-tab",
        selected_className="single-tab--selected",
    )

def buat_grafik_line(df, x, y, text, color, title):
    return px.line(
        df,
        x=x,
        y=y,
        markers=True,
        color = color,
        text = text,
        title = title
    )

def buat_grafik_bar(df, x, y,color):
    return px.bar(
        df, 
        x=x, 
        y=y,
        color=color
    )

def buat_grafik_bar2(df, x, y):
    return px.bar(
        df, 
        x=x, 
        y=y
    )

def buat_grafik_pie(df, values, names):
    return px.pie(
        df, 
        hole=.5, 
        values=values, 
        names=names
    )

def buat_navigasi():
    dbc.Nav(
        [
            dbc.NavLink("Home", href="/", active="exact"),
            dbc.NavLink("User", href="/", active="exact"),
            dbc.NavLink("Social Media", href="/", active="exact"),
        ],
        vertical = True,
        pills = True
    )

def genSankey(df,cat_cols=[],value_cols='',title='Sankey Diagram'):
    # maximum of 6 value cols -> 6 colors
    colorPalette = ['#4B8BBE','#306998','#FFE873','#FFD43B','#646464']
    labelList = []
    colorNumList = []
    for catCol in cat_cols:
        labelListTemp =  list(set(df[catCol].values))
        colorNumList.append(len(labelListTemp))
        labelList = labelList + labelListTemp
        
    # remove duplicates from labelList
    labelList = list(dict.fromkeys(labelList))
    
    # define colors based on number of levels
    colorList = []
    for idx, colorNum in enumerate(colorNumList):
        colorList = colorList + [colorPalette[idx]]*colorNum
        
    # transform df into a source-target pair
    for i in range(len(cat_cols)-1):
        if i==0:
            sourceTargetDf = df[[cat_cols[i],cat_cols[i+1],value_cols]]
            sourceTargetDf.columns = ['source','target','count']
        else:
            tempDf = df[[cat_cols[i],cat_cols[i+1],value_cols]]
            tempDf.columns = ['source','target','count']
            sourceTargetDf = pd.concat([sourceTargetDf,tempDf])
        sourceTargetDf = sourceTargetDf.groupby(['source','target']).agg({'count':'sum'}).reset_index()
        
    # add index for source-target pair
    sourceTargetDf['sourceID'] = sourceTargetDf['source'].apply(lambda x: labelList.index(x))
    sourceTargetDf['targetID'] = sourceTargetDf['target'].apply(lambda x: labelList.index(x))
    
    # creating the sankey diagram
    data = dict(
        type='sankey',
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(
            color = "black",
            width = 0.5
          ),
          label = labelList,
          color = colorList
        ),
        link = dict(
          source = sourceTargetDf['sourceID'],
          target = sourceTargetDf['targetID'],
          value = sourceTargetDf['count']
        )
      )
    
    layout =  dict(
        title = title,
        font = dict(
          size = 10
        )
    )
       
    fig = dict(data=[data], layout=layout)
    return fig

# tblSuksesPra=buat_table(dfTransaksiSuksesPra)
# tblSuksesPasca=buat_table(dfTransaksiSuksesPasca)
# tblSuksesLainnya=buat_table(dfTransaksiSuksesLainnya)
# tblRefundPra=buat_table(dfTransaksiRefundPra)
# tblRefundLainnya=buat_table(dfTransaksiRefundLainnya)

# tab_tbl_sukses_pra = buat_tab(tblSuksesPra,"Prabayar","pra")
# tab_tbl_sukses_pasca = buat_tab(tblSuksesPasca,"Pascabayar","pasca")
# tab_tbl_sukses_lainnya = buat_tab(tblSuksesLainnya,"Lain-Lain","lainnya")

dfPraSukses = dfTransaksiSuksesPra2.groupby(['tgl_transaksi','customer_id','kategori_produk','status','transaksi']).sum(['jumlah_transaksi','harga_beli','harga_jual']).sort_values('tgl_transaksi').reset_index()
#dfPraRefund = dfTransaksiRefundPra2.groupby(['tgl_transaksi','customer_id','kategori_produk','status'],'transaksi').sum(['jumlah_transaksi','harga_beli','harga_jual']).sort_values('tgl_transaksi').reset_index()
dfPascaSukses = dfTransaksiSuksesPasca2.groupby(['tgl_transaksi','customer_id','kategori_produk','status','transaksi']).sum(['jumlah_transaksi','harga_jual','margin']).sort_values('tgl_transaksi').reset_index()
dfLainnyaSukses = dfTransaksiSuksesLainnya2.groupby(['tgl_transaksi','customer_id','status','kategori_produk','transaksi']).sum(['jumlah_transaksi','harga_jual']).sort_values('tgl_transaksi').reset_index()
dfLainnyaRefund = dfTransaksiRefundLainnya2.groupby(['tgl_transaksi','customer_id','status','kategori_produk','transaksi']).sum(['jumlah_transaksi','harga_jual']).sort_values('tgl_transaksi').reset_index()
# if dfTransaksiRefundPasca.count() > 0:
#     dfPascaRefund = dfTransaksiRefundPasca.groupby(['tanggal_transaksi','status_transaksi']).sum(['jumlah_transaksi','total_transaksi','margin']).sort_values('tanggal_transaksi').reset_index()
# tbl_graph_sukses = buat_tab(buat_grafik_line(dfPraSukses, "tgl_transaksi", "jumlah_transaksi", "jumlah_transaksi", "keterangan","Transaksi Prabayar Sukses"),"Grafik Prabayar","line-pra")
# tbl_graph_refund = buat_tab(buat_grafik_line(dfPraRefund, "tgl_transaksi", "jumlah_transaksi", "jumlah_transaksi", "keterangan","Transaksi Prabayar Refund"),"Grafik Prabayar","line-pra-refund")
# tab_tbl_refund_pra = buat_tab(tblRefundPra,"Prabayar","pra")
# tab_tbl_refund_lainnya = buat_tab(tblRefundLainnya,"Lain-Lain","lainnya")

dfPraSukses['tgl_transaksi'] = pd.to_datetime(dfPraSukses['tgl_transaksi'])
dfPraSukses['week'] = dfPraSukses['tgl_transaksi'].dt.isocalendar().week
dfPraSukses['month'] = dfPraSukses['tgl_transaksi'].dt.month
dfPraSukses['year'] = dfPraSukses['tgl_transaksi'].dt.year

# dfPraRefund['tgl_transaksi'] = pd.to_datetime(dfPraRefund['tgl_transaksi'])
# dfPraRefund['week'] = dfPraRefund['tgl_transaksi'].dt.isocalendar().week
# dfPraRefund['month'] = dfPraRefund['tgl_transaksi'].dt.month
# dfPraRefund['year'] = dfPraRefund['tgl_transaksi'].dt.year

dfPascaSukses['tgl_transaksi'] = pd.to_datetime(dfPascaSukses['tgl_transaksi'])
dfPascaSukses['week'] = dfPascaSukses['tgl_transaksi'].dt.isocalendar().week
dfPascaSukses['month'] = dfPascaSukses['tgl_transaksi'].dt.month
dfPascaSukses['year'] = dfPascaSukses['tgl_transaksi'].dt.year

dfLainnyaSukses['tgl_transaksi'] = pd.to_datetime(dfLainnyaSukses['tgl_transaksi'])
dfLainnyaSukses['week'] = dfLainnyaSukses['tgl_transaksi'].dt.isocalendar().week
dfLainnyaSukses['month'] = dfLainnyaSukses['tgl_transaksi'].dt.month
dfLainnyaSukses['year'] = dfLainnyaSukses['tgl_transaksi'].dt.year

dfLainnyaRefund['tgl_transaksi'] = pd.to_datetime(dfLainnyaRefund['tgl_transaksi'])
dfLainnyaRefund['week'] = dfLainnyaRefund['tgl_transaksi'].dt.isocalendar().week
dfLainnyaRefund['month'] = dfLainnyaRefund['tgl_transaksi'].dt.month
dfLainnyaRefund['year'] = dfLainnyaRefund['tgl_transaksi'].dt.year

dfSummary = pd.concat([dfPraSukses, dfPascaSukses, dfLainnyaSukses])

# table_tabs = dcc.Tabs(
#     [tab_tbl_sukses_pra,tab_tbl_sukses_pasca,tab_tbl_sukses_lainnya],
#     className="tabs-container1",
#     id="table-tabs",
#     value="pra"
# )
# table_tabs.style={'gridArea':'tables'}
customerList = dfLBCustomer2[['customer_id','name']].drop_duplicates().to_dict('records')
card_pendapatan = dbc.Card(
    dbc.CardBody(
        [
            html.H2("Pendapatan"),
            html.H2(dfSummary['jumlah_transaksi'].astype('int').sum())
        ]
    )
)

card_refund = dbc.Card(
    dbc.CardBody(
        [
            html.H2("Refund"),
            html.H2(dfPraSukses['jumlah_transaksi'].astype('int').sum()+dfPascaSukses['jumlah_transaksi'].astype('int').sum()+dfLainnyaSukses['jumlah_transaksi'].astype('int').sum())
        ]
    )
)

card_trans_sukses = dbc.Card(
    dbc.CardBody(
        [
            html.H2("Transaksi Sukses"),
            html.H2("This")
        ]
    )
)

card_trans_gagal = dbc.Card(
    dbc.CardBody(
        [
            html.H2("Transaksi Gagal"),
            html.H2("This")
        ]
    )
)

cards = dbc.Row(
    [
        dbc.Col(card_pendapatan, width=4, className="col-md-4"),
        dbc.Col(card_refund, width=4, className="col-md-4"),
        dbc.Col(card_trans_sukses, width=4, className="col-md-4"),
        dbc.Col(card_trans_gagal, width=4, className="col-md-8"),
    ]
)
#cards.style={'gridArea':'card'}

sidebar = html.Div(
    [
    dbc.Row(
            [
                html.P('Periode')
            ],
    ),
    dbc.Row(
            html.Div(
                dcc.DatePickerRange(
                    id="periode",
                    start_date=first_day,
                    end_date=last_day,
                    display_format="YYYY-MM-DD"
                )
            )
    ),
    dbc.Row(
            [
                html.P('Group By')
            ]
    ),
    dbc.Row(
            html.Div(
                dcc.RadioItems(
                    id='radio_scope',
                    options={
                    'D': 'Daily',
                    'W': 'Weekly',
                    'M': 'Monthly'
                    },
                    value='D'
                )
            )
    ),
    dbc.Row(
            [
                html.P('Search Customer :')
            ]
    ),
    dbc.Row(
        html.Div(
            dcc.Dropdown(
                id="customer_id",
                options=[{'label': i['name'], 'value': i['customer_id']} for i in customerList],
                value='all_values',
                multi=True
            ),
        )
    ),
    card_pendapatan,
    card_refund,
    card_trans_sukses,
    card_trans_gagal
    ]
)
#SUMMARY TRANS
graph_pie_sum = dcc.Graph(id='pie_sum')
graph_pie_trans_sum = dcc.Graph(id='pie_trans_sum')
graph_bar_sum = dcc.Graph(id='bar_sum')
graph_pie_pra = dcc.Graph(id='pie_pra')
graph_bar_pra = dcc.Graph(id='bar_pra')
graph_pie_pasca = dcc.Graph(id='pie_pasca')
graph_bar_pasca = dcc.Graph(id='bar_pasca')
graph_pie_lain = dcc.Graph(id='pie_lain')
graph_bar_lain = dcc.Graph(id='bar_lain')
graph_pie_ref_lain = dcc.Graph(id='pie_ref_lain')
graph_bar_ref_lain = dcc.Graph(id='bar_ref_lain')
## SUMMARY CUST
graph_pie_status_cust= dcc.Graph(id='status_cust')
graph_pie_statusIn_cust = dcc.Graph(id='active_non_cust')

container_sum = html.Div([
        graph_pie_sum, graph_pie_trans_sum, graph_bar_sum
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

container_pra = html.Div([
        graph_pie_pra, graph_bar_pra
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

container_pasca = html.Div([
        graph_pie_pasca, graph_bar_pasca
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

container_lain = html.Div([
        graph_pie_lain, graph_bar_lain
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

container_ref_lain = html.Div([
        graph_pie_ref_lain, graph_bar_ref_lain
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

container_cust_sum = html.Div([
        graph_pie_status_cust, graph_pie_statusIn_cust
    ]
    ,style={
        'display':'grid',
        'gridTemplateAreas':'"graph1" "graph2"',
        'gridTemplateColumns':"35vw 35vw",
        'columnGap':'6px',
    }
)

transaksi_tabs = dcc.Tabs([
        dcc.Tab(label='Summary', children=[
            dcc.Dropdown(
                    id="pilih_x_sum",
                        options=list(
                            dfSummary.drop(columns=['week', 'month','year']).select_dtypes(include="number").columns.unique()
                        ),
                    value='all_values'
            ),
            dcc.Graph(
                    id='sunkey_pendapatan'
            ),
            container_sum
        ]),
        dcc.Tab(label='Prabayar', children=[
            dcc.Dropdown(
                    id="pilih_x_pra",
                        options=list(
                            dfPraSukses.drop(columns=['week', 'month','year']).select_dtypes(include="number").columns.unique()
                        ),
                    value='all_values'
            ),
            dcc.Graph(
                    id='timeline_pra_sukses'
            ),
            container_pra
        ]),
        dcc.Tab(label='Pascabayar', children=[
            dcc.Dropdown(
                id="pilih_x_pasca",
                    options=list(
                        dfPascaSukses.drop(columns=['week', 'month','year']).select_dtypes(include="number").columns.unique()
                    ),
                value='all_values'
            ),
            dcc.Graph(
                id='timeline_pasca_sukses'
            ),
            container_pasca
        ]),
        dcc.Tab(label='Lainnya', children=[
            dcc.Dropdown(
                id="pilih_x_lainnya",
                    options=list(
                        dfLainnyaSukses.drop(columns=['week', 'month','year']).select_dtypes(include="number").columns.unique()
                    ),
                value='all_values'
            ),
            dcc.Graph(
                id='timeline_lainnya_sukses'
            ),
            container_lain
        ]),
        dcc.Tab(label='Deposit/Refund Lainnya', children=[
            dcc.Dropdown(
                id="pilih_x_depo_ref",
                    options=list(
                        dfLainnyaRefund.drop(columns=['week', 'month','year']).select_dtypes(include="number").columns.unique()
                    ),
                value='all_values'
            ),
            dcc.Graph(
                id='timeline_lainnya_refund'
            ),
            container_ref_lain
        ])
])
#graph_tabs.style={'gridArea':'sales'}

customer_tabs = dcc.Tabs([
    dcc.Tab(label='Summary', children=[
        dcc.Graph(
            id='sankey_cust'
        ),
        dcc.Graph(
            id='tunnel_cust'
        ),
        dcc.Graph(
            id='scatter_cust'
        ),
        container_cust_sum
    ]),
    dcc.Tab(label='Customer Transaksi', children=[
        dcc.Dropdown(
                id="pilih_x_cust",
                    options=list(
                        dfPraSukses.select_dtypes(include="number").columns.unique()
                    ),
                value='all_values'
        ),
        dcc.Graph(
            id='timeline_customer'
        ),
        dcc.Graph(
            id='pie_customer'
        ),
        dcc.Graph(
            id='bar_customer'
        )
    ])
])

sosmed_tabs = dcc.Tabs([
    dcc.Tab(label='Summary', children=[
        
    ]),
    dcc.Tab(label='Google Analytics', children=[

    ]),
    dcc.Tab(label='Facebook & IG Ads', children=[
        
    ])
])

main_tabs = dcc.Tabs([
    dcc.Tab(label='Transaksi', children=[
        transaksi_tabs
    ]),
    dcc.Tab(label='Customer', children=[
        customer_tabs
    ]),
    dcc.Tab(label='Social Media', children=[
        sosmed_tabs    
    ])    
])
main_tabs.style={'gridArea':'sales'}

title = html.H2('Serpul Dashboard', style={
    'backgroundColor':'tan',
    'fontFamily':'verdana',
    'textAlign':'center'
})

container = html.Div([sidebar,main_tabs],
    style={
        'display':'grid',
        'gridTemplateAreas':'"sidebar graph" "detail tables"',
        'gridTemplateColumns':"30vw 70vw",
        'columnGap':'6px',
    }
)

layout = html.Div([title, container])

app = Dash(__name__)
app.layout = layout

@app.callback(
    Output("sunkey_pendapatan","figure"),
    Output("pie_sum","figure"),
    Output("bar_sum","figure"),
    Output("pie_trans_sum","figure"), 
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_sum", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_sum(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra
    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"
    
    if not kode or kode =="all_values" or len(kode) == 0:
        dfSukses = dfSummary.loc[dfSummary['tgl_transaksi'].between(start,end)]
    else:
        dfSukses = dfSummary.loc[dfSummary['tgl_transaksi'].between(start,end)].query("customer_id in @kode")

    dfSukses['pendapatan'] = 'Pendapatan'
    if not x or x =="all_values":
        figPieSum = buat_grafik_pie(dfSukses.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "status")
        figPieTransSum = buat_grafik_pie(dfSukses.groupby([group_scope,'transaksi']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "transaksi")
        figBarSum = buat_grafik_bar(dfSukses.groupby(['kategori_produk','status']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","status")
        figSankeySum = genSankey(dfSukses,cat_cols=['pendapatan','status','transaksi','kategori_produk'],value_cols='jumlah_transaksi',title='Grafik Sankey Pendapatan')
    else:
        figPieSum = buat_grafik_pie(dfSukses.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), x,"status")
        figPieTransSum = buat_grafik_pie(dfSukses.groupby([group_scope,'transaksi']).agg({x:'sum'}).reset_index(), x, "transaksi")
        figBarSum = buat_grafik_bar(dfSukses.groupby(['kategori_produk','status']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk", x,"status")
        figSankeySum = genSankey(dfSukses,cat_cols=['pendapatan','status','transaksi','kategori_produk'],value_cols=x,title='Grafik Sankey Pendapatan')
    
    return figSankeySum,figPieSum,figBarSum,figPieTransSum

@app.callback(
    Output("timeline_pra_sukses","figure"), 
    Output("pie_pra","figure"), 
    Output("bar_pra","figure"), 
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_pra", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_pra(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra
    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"
    
    if not kode or kode =="all_values" or len(kode) == 0:
        dfSukses = dfPraSukses.loc[dfPraSukses['tgl_transaksi'].between(start,end)]
    else:
        dfSukses = dfPraSukses.loc[dfPraSukses['tgl_transaksi'].between(start,end)].query("customer_id in @kode")

    if not x or x =="all_values":
        figLinePra = buat_grafik_line(dfSukses.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), group_scope, "jumlah_transaksi", "jumlah_transaksi", "status","Transaksi Prabayar")
        figPiePra = buat_grafik_pie(dfSukses.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "status")
        figBarPra = buat_grafik_bar(dfSukses.groupby(['kategori_produk','status']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","status")
    else:
        figLinePra = buat_grafik_line(dfSukses.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), group_scope, x, x, "status","Transaksi Prabayar")
        figPiePra = buat_grafik_pie(dfSukses.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), x,"status")
        figBarPra = buat_grafik_bar(dfSukses.groupby(['kategori_produk','status']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk", x,"status")

    return figLinePra,figPiePra,figBarPra

@app.callback(
    Output("timeline_pasca_sukses","figure"),
    Output("pie_pasca","figure"), 
    Output("bar_pasca","figure"), 
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_pasca", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_pasca(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra

    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"

    if not kode or kode =="all_values" or len(kode) == 0:
        dfSuksesPasca = dfPascaSukses.loc[dfPascaSukses['tgl_transaksi'].between(start,end)]
    else:
        dfSuksesPasca = dfPascaSukses.loc[dfPascaSukses['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
    
    if not x or x =="all_values":
        figLinePasca = buat_grafik_line(dfSuksesPasca.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), group_scope, "jumlah_transaksi", "jumlah_transaksi", "status","Transaksi Pascabayar")
        figPiePasca = buat_grafik_pie(dfSuksesPasca.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "status")
        figBarPasca = buat_grafik_bar(dfSuksesPasca.groupby(['kategori_produk','status']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","status")
    else:
        figLinePasca = buat_grafik_line(dfSuksesPasca.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), group_scope, x, x, "status","Transaksi Pascabayar")
        figPiePasca = buat_grafik_pie(dfSuksesPasca.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), x, "status")
        figBarPasca = buat_grafik_bar(dfSuksesPasca.groupby(['kategori_produk','status']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk",x,"status")

    return figLinePasca,figPiePasca,figBarPasca

@app.callback(
    Output("timeline_lainnya_sukses","figure"), 
    Output("pie_lain","figure"), 
    Output("bar_lain","figure"),
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_lainnya", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_lainnya(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra
    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"

    if not kode or kode =="all_values" or len(kode) == 0:
        dfLainSukses = dfLainnyaSukses.loc[(dfLainnyaSukses['tgl_transaksi'].between(start,end))]
    else:
        dfLainSukses = dfLainnyaSukses.loc[(dfLainnyaSukses['tgl_transaksi'].between(start,end))].query("customer_id in @kode")
    if not x or x =="all_values":
        figLineLain = buat_grafik_line(dfLainSukses.groupby([group_scope,'kategori_produk']).agg({'jumlah_transaksi':'sum'}).reset_index(), group_scope, "jumlah_transaksi", "jumlah_transaksi","kategori_produk","Transaksi Lainnya")
        figPieLain = buat_grafik_pie(dfLainSukses.groupby([group_scope,'kategori_produk']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "kategori_produk")
        figBarLain = buat_grafik_bar(dfLainSukses.groupby(['kategori_produk']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","kategori_produk")
    else:
        figLineLain = buat_grafik_line(dfLainSukses.groupby([group_scope,'kategori_produk']).agg({x:'sum'}).reset_index(), group_scope, x, x,"kategori_produk","Transaksi Lainnya")
        figPieLain = buat_grafik_pie(dfLainSukses.groupby([group_scope,'kategori_produk']).agg({x:'sum'}).reset_index(), x, "kategori_produk")
        figBarLain = buat_grafik_bar(dfLainSukses.groupby(['kategori_produk']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk",x,"kategori_produk")

    return figLineLain,figPieLain,figBarLain

@app.callback(
    Output("timeline_lainnya_refund","figure"), 
    Output("pie_ref_lain","figure"), 
    Output("bar_ref_lain","figure"),
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_depo_ref", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_dep_ref_lainnya(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra
    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"

    if not kode or kode =="all_values" or len(kode) == 0:
        dfLainDepRefSukses = dfLainnyaRefund.loc[(dfLainnyaRefund['tgl_transaksi'].between(start,end))]
    else:
        dfLainDepRefSukses = dfLainnyaRefund.loc[(dfLainnyaRefund['tgl_transaksi'].between(start,end))].query("customer_id in @kode")

    if not x or x =="all_values":
        figLineDepLain = buat_grafik_line(dfLainDepRefSukses.groupby([group_scope,'kategori_produk']).agg({'jumlah_transaksi':'sum'}).reset_index(), group_scope, "jumlah_transaksi", "jumlah_transaksi","kategori_produk","Transaksi Deposit/Refund Lainnya")
        figPieDepLain = buat_grafik_pie(dfLainDepRefSukses.groupby([group_scope,'kategori_produk']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "kategori_produk")
        figBarDepLain = buat_grafik_bar(dfLainDepRefSukses.groupby(['kategori_produk']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","kategori_produk")
    else:
        figLineDepLain = buat_grafik_line(dfLainDepRefSukses.groupby([group_scope,'kategori_produk']).agg({x:'sum'}).reset_index(), group_scope, x, x,"kategori_produk","Transaksi Lainnya")
        figPieDepLain = buat_grafik_pie(dfLainDepRefSukses.groupby([group_scope,'kategori_produk']).agg({x:'sum'}).reset_index(), x, "kategori_produk")
        figBarDepLain = buat_grafik_bar(dfLainDepRefSukses.groupby(['kategori_produk']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk",x,"kategori_produk")

    return figLineDepLain,figPieDepLain,figBarDepLain

@app.callback(
    Output("sankey_cust","figure"),
    Output("tunnel_cust","figure"),
    Output("scatter_cust","figure"), 
    Output("status_cust","figure"),
    Output("active_non_cust","figure"),     
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("customer_id", "value")
)

def customer_sum(start,end, kode):
    if not start or not end:
        PreventUpdate
    regis = dfLBCustomer2[dfLBCustomer2['name']!="TokoN"]
    depo = dfLainnyaRefund[dfLainnyaRefund['kategori_produk']=="Deposit"]
    langganan = dfLainnyaSukses[dfLainnyaSukses['kategori_produk']=="By. Langganan"]
    payment = dfLainnyaSukses[dfLainnyaSukses['kategori_produk']=="Payment"]
    addon = dfLainnyaSukses[dfLainnyaSukses['kategori_produk']=="Addon"]

    dfLainSukses = dfLainnyaSukses.loc[(dfLainnyaSukses['tgl_transaksi'].between(start,end))]
    figCustTunnel = go.Figure(go.Funnel(
        y = ["Register", "Deposit","By. Langganan","Payment", "Addon"],
        x = [regis[regis['tgl_transaksi'].between(start,end)].drop_duplicates("customer_id").shape[0], \
            depo[depo['tgl_transaksi'].between(start,end)].drop_duplicates("customer_id").shape[0], \
            langganan[langganan['tgl_transaksi'].between(start,end)].drop_duplicates("customer_id").shape[0] \
            ,payment[payment['tgl_transaksi'].between(start,end)].drop_duplicates("customer_id").shape[0], \
            addon[addon['tgl_transaksi'].between(start,end)].drop_duplicates("customer_id").shape[0]],
        textposition = "inside",
        textinfo = "value+percent initial",
        opacity = 0.65, marker = {"color": ["deepskyblue", "lightsalmon", "tan", "teal", "silver"],
        "line": {"width": [4, 2, 2, 3, 1, 1], "color": ["wheat", "wheat", "blue", "wheat", "wheat"]}},
        connector = {"line": {"color": "royalblue", "dash": "dot", "width": 3}})
    )

    statusCust = dfLBCustomer2[dfLBCustomer2['name']!="TokoN"]
    figCustStatus = buat_grafik_pie(statusCust.groupby(['status']).agg({'customer_id':'count'}).reset_index(), "customer_id","status")

    #statusCust = dfLBCustomerAktif2[dfLBCustomerAktif2['name']!="TokoN"]
    figCustStatusIn = buat_grafik_pie(dfLBCustomerAktif2.groupby(['status_tenant']).agg({'customer_id':'count'}).reset_index(), "customer_id","status_tenant")
    
    dfLBCustomer2["nilai_mutasi_transaksi"] = dfLBCustomer2["nilai_mutasi_transaksi"].astype(int)
    dfLBCustomer2["deposit"] = dfLBCustomer2["deposit"].astype(int)
    dfLBCustScatter = dfLBCustomer2.loc[(dfLBCustomer2['tgl_transaksi'].between(start,end))]
    figCustScatter = px.scatter(dfLBCustScatter.groupby(['customer_id','paket']).agg({'deposit':'sum', 'nilai_mutasi_transaksi':'sum'}).reset_index(), x="nilai_mutasi_transaksi", y="deposit", color="paket",
                 size='nilai_mutasi_transaksi')

    ### SANKEY CUST
    dfLBSaldoAwal = dfLBCustomer2[['tgl_transaksi','customer_id','saldo_awal']]
    dfLBDeposit= dfLBCustomer2[['tgl_transaksi','customer_id','deposit']]
    dfLBRefund = dfLBCustomer2[['tgl_transaksi','customer_id','refund']]
    dfLBTransaksi= dfLBCustomer2[['tgl_transaksi','customer_id','nilai_mutasi_transaksi']]
    dfSummaryHarga = dfSummary[['tgl_transaksi','customer_id','harga_jual']]
    dfSummaryLaba = dfSummary[['tgl_transaksi','customer_id','laba']]


    if not kode or kode =="all_values" or len(kode) == 0:
        dfLBSaldoAwal = dfLBSaldoAwal[dfLBSaldoAwal['tgl_transaksi'].between(start,end)]
        dfLBDeposit = dfLBDeposit[dfLBDeposit['tgl_transaksi'].between(start,end)]
        dfLBRefund = dfLBRefund[dfLBRefund['tgl_transaksi'].between(start,end)]
        dfLBTransaksi = dfLBTransaksi[dfLBTransaksi['tgl_transaksi'].between(start,end)]
        dfSummaryHarga = dfSummaryHarga[dfSummaryHarga['tgl_transaksi'].between(start,end)]
        dfSummaryLaba = dfSummaryLaba[dfSummaryLaba['tgl_transaksi'].between(start,end)]
    else:
        dfLBSaldoAwal = dfLBSaldoAwal[dfLBSaldoAwal['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
        dfLBDeposit = dfLBDeposit[dfLBDeposit['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
        dfLBRefund = dfLBRefund[dfLBRefund['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
        dfLBTransaksi = dfLBTransaksi[dfLBTransaksi['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
        dfSummaryHarga = dfSummaryHarga[dfSummaryHarga['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
        dfSummaryLaba = dfSummaryLaba[dfSummaryLaba['tgl_transaksi'].between(start,end)].query("customer_id in @kode")
    
    dfLBSaldoAwal = dfLBSaldoAwal.rename(columns = {"saldo_awal": "nilai"})
    dfLBSaldoAwal['nilai'] = dfLBSaldoAwal['nilai'].astype('int')
    dfLBSaldoAwal['LB'] = "Saldo Awal"
    dfLBSaldoAwal['LB1'] = "COH"
    dfLBSaldoAwal['LB2'] = "Transaksi"

    
    dfLBDeposit = dfLBDeposit.rename(columns = {"deposit": "nilai"})
    dfLBDeposit['nilai'] = dfLBDeposit['nilai'].astype('int')
    dfLBDeposit['LB'] = "Deposit"
    dfLBDeposit['LB1'] = "COH"
    dfLBDeposit['LB2'] = "Transaksi"

    
    dfLBRefund = dfLBRefund.rename(columns = {"refund": "nilai"})
    dfLBRefund['nilai'] = dfLBRefund['nilai'].astype('int')
    dfLBRefund['LB'] = "Refund"
    dfLBRefund['LB1'] = "COH"
    dfLBRefund['LB2'] = "Transaksi"

    
    dfLBTransaksi = dfLBTransaksi.rename(columns = {"nilai_mutasi_transaksi": "nilai"})
    dfLBTransaksi['nilai'] = dfLBTransaksi['nilai'].astype('int')
    dfLBTransaksi['LB1'] = "COH"
    dfLBTransaksi['LB2'] = "Transaksi"

    
    dfLBNilaiTrans = dfSummaryHarga.rename(columns = {"harga_jual": "nilai"})
    dfLBNilaiTrans['nilai'] = dfLBNilaiTrans['nilai'].astype('int')
    dfLBNilaiTrans['LB2'] = "Transaksi"
    dfLBNilaiTrans['LB3'] = "Penjualan"

    
    dfLBNilaiLaba = dfSummaryLaba.rename(columns = {"laba": "nilai"})
    dfLBNilaiLaba['nilai'] = dfLBNilaiLaba['nilai'].astype('int')
    dfLBNilaiLaba['LB2'] = "Transaksi"
    dfLBNilaiLaba['LB3'] = "Laba"

    dfSummaryLB = pd.concat([dfLBSaldoAwal, dfLBDeposit, dfLBRefund, dfLBTransaksi, dfLBNilaiTrans,dfLBNilaiLaba])
    figSankeyCust=genSankey(dfSummaryLB,cat_cols=['LB','LB1','LB2','LB3'],value_cols="nilai",title='Grafik Sankey COH Customer')
    #### SANKEY CUST

    return figSankeyCust, figCustTunnel, figCustScatter, figCustStatus,figCustStatusIn 

@app.callback(
    Output("timeline_customer","figure"), 
    Output("pie_customer","figure"), 
    Output("bar_customer","figure"),
    [Input("periode","start_date"),Input("periode","end_date")],
    Input("radio_scope", "value"),
    Input("pilih_x_cust", "value"),
    Input("customer_id", "value")
)

def graph_transaksi_customer(start,end,scope,x,kode):
    if not start or not end:
        PreventUpdate
    # pra
    if scope == 'W':
        group_scope = "week"
    elif scope == 'M':
        group_scope = "month"
    else:
        group_scope = "tgl_transaksi"
    
    if not kode or kode =="all_values" or len(kode) == 0:
        dfTransCustPra = dfPraSukses.loc[(dfPraSukses['tgl_transaksi'].between(start,end))]
    else:
        dfTransCustPra = dfPraSukses.loc[(dfPraSukses['tgl_transaksi'].between(start,end))].query("customer_id in @kode")
    
    if not x or x =="all_values":
        figLinePraCust = buat_grafik_line(dfTransCustPra.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), group_scope, "jumlah_transaksi", "jumlah_transaksi", "status","Transaksi Prabayar")
        figPiePraCust = buat_grafik_pie(dfTransCustPra.groupby([group_scope,'status']).agg({'jumlah_transaksi':'sum'}).reset_index(), "jumlah_transaksi", "status")
        figBarPraCust = buat_grafik_bar(dfTransCustPra.groupby(['kategori_produk','status']).agg({'jumlah_transaksi':'sum'}).sort_values('jumlah_transaksi').reset_index(), "kategori_produk","jumlah_transaksi","status")
    else:
        figLinePraCust = buat_grafik_line(dfTransCustPra.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), group_scope, x, x, "status","Transaksi Prabayar")
        figPiePraCust = buat_grafik_pie(dfTransCustPra.groupby([group_scope,'status']).agg({x:'sum'}).reset_index(), x,"status")
        figBarPraCust = buat_grafik_bar(dfTransCustPra.groupby(['kategori_produk','status']).agg({x:'sum'}).sort_values(x).reset_index(), "kategori_produk", x,"status")

    return figLinePraCust,figPiePraCust,figBarPraCust


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=9999)
    

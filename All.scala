rawConsumer.scala
curatedLoad.insertRecords(flgLegDF, tableName, columnFamily, schemaFields, 16, 17)

Curated Load.scala
  def putFunc(put: Put, cf: String, colName: String, tsValue: String, colValue: String): Unit = {
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), tsValue.toLong, Bytes.toBytes(colValue))
  }

  def insertRecords(fltLegDF: DataFrame, tableName: String, columnFamily: String, schemaFields: String, rkIndex: Int, timeStpIndex: Int): Unit = {

    val finRDD = fltLegDF.rdd.map(_.mkString(";,;"))
    val conf: Configuration = HBaseConfiguration.create()
    val hbaseContext: HBaseContext = new HBaseContext(sc, conf)
    val colNames = schemaFields.split(",")
    hbaseContext.bulkPut[String](finRDD,
      TableName.valueOf(tableName),
      (putRecord) => {
        var colIndex = 0
        var put = new Put(Bytes.toBytes(putRecord.split(";,;")(rkIndex)))
        for (colName <- colNames) {
          putFunc(put, columnFamily, colName, putRecord.split(";,;")(timeStpIndex), putRecord.split(";,;")(colIndex))
          colIndex = colIndex + 1
        }
        put
      })
  }

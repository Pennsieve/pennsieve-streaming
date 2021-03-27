package com.blackfynn.streaming.query

case class ResultDimensions(resultStart: Long, resultEnd: Long, queryStart: Long, queryEnd: Long) {
  //query       ---
  //result   ---------
  def queryRich: Boolean =
    (resultStart <= queryStart) && (resultEnd >= queryEnd)

  //query    --------
  //result     ---
  def queryPoor: Boolean =
    (resultStart >= queryStart) && (resultEnd <= queryEnd)

  //query    ----
  //result     -------
  def queryOverlapRight: Boolean =
    (resultStart >= queryStart) && (resultEnd >= queryEnd)

  //query        -----
  //result   ------
  def queryOverlapLeft: Boolean =
    (resultStart <= queryStart) && (resultEnd <= queryEnd)

  //query            -----
  //result   ----
  def queryDisjoint: Boolean =
    (resultStart > queryEnd) || (resultEnd < queryStart)

  def queryPerfect: Boolean =
    (resultStart == queryStart) && (resultEnd == queryEnd)

}

/* global require */
/* global module */
/* global process */
/* global console */

var weblogMssql = function(setup) {
  setup.host  = setup.host ? setup.host : require('ip').address()
  setup.topic = setup.domain+'.'+setup.host+'.'+setup.service

  var _ = require('lodash')
  var when = require('when')
  var mssql = require('mssql')

  var sqlconn = new mssql.Connection(setup.dbconnection, function(err) {
      if (err) { console.log('err', err); process.exit(8) }
  })

  var autobahn = require('autobahn')

  var connection = new autobahn.Connection({
    url: process.argv[2] || 'ws://127.0.0.1:8080/ws',
    realm: process.argv[3] || 'weblog'
  })

  var main = function(session) {
    session.subscribe('discover', function() {
      session.publish('announce', [_.pick(setup, 'domain', 'host', 'service', 'topic')])
    })

    session.register(setup.topic+'.header', function() {
      return setup.headers
    })

    session.register(setup.topic+'.reload', function(args) {
      var d = when.defer()
      var controls = args[0]
      if (controls.offset < 0) controls.offset = 0
      var table = controls.header
      if (table.select) table.view = '(' + table.select + ') c23f95fa643c3ae54de266c2b301d0'
      var wherearr = []
      wherearr.push(table.where)
      if (controls.begin)  wherearr.push(table.fields[controls.rangefield]+' >= \''+controls.begin+'\'')
      if (controls.end)    wherearr.push(table.fields[controls.rangefield]+' <= \''+controls.end+'\'')
      if (controls.filter) wherearr.push(table.fields[controls.filterfield]+' LIKE \'%'+controls.filter+'%\'')
      var where = wherearr.join(' AND ')
      var query = 'SELECT TOP ' + controls.count + ' ' + table.fields + ' FROM ' + table.view + ' WHERE ' + where + ' ORDER BY ' + table.orderby

      var request = new mssql.Request(sqlconn)
      request.query(query).then(function(rows) {
        var res = []
        _.each(rows, function(row) { res.push(_.values(row)) })
//        console.dir(res)
        d.resolve(res)
      }).catch(function(err) {
        console.log('err', err)
      })
      return d.promise
    })
  }

  connection.onopen = main

  connection.open()
}

module.exports = weblogMssql

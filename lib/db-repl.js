var
  repl = require('repl'),
  util = require('util'),
  _ = require('lodash'),
  Promise = require('bluebird'),
  MysqlSchemaInfo = require('./schema-info/mysql'),
  schemaInfos = {
    mysql: MysqlSchemaInfo
  };

module.exports = {

  start: function start(knex, options) {
    return createRepl(knex, options);
  }

};

function getClientName(knex) {
  //todo: get upstream knex to expose the client name without having to do this hack
  var match = /\[object Knex:(.*)\]/.exec(knex.toString());
  return match ? match[1] : null;
}

function getSchemaInfo(client) {
  return schemaInfos[client];
}

function createRepl(knex, options) {
  var
    opts = options || {},
    clientName = getClientName(knex),
    SchemaInfo = getSchemaInfo(clientName),
    schemaInfo = new SchemaInfo(knex),
    databaseName = knex.client.databaseName,
    prompt = opts.prompt || '[DB] > ';

  return Promise.props({
    metaSchemaModels: schemaInfo.createMetaSchemaModels(),
    schemaModels: schemaInfo.createSchemaModels(databaseName),
    columnInfo: schemaInfo.getColumnInfo(),
    tableInfo: schemaInfo.getTableInfo(),
    viewInfo: schemaInfo.getViewInfo(),
    fullTableInfo: schemaInfo.getFullTableInfo(),
    tableGraph: schemaInfo.getTableGraph(),
    keyColumnDependencies: schemaInfo.getKeyColumnDependencies()
  }).then(function(result) {
    var dbRepl = repl.start({
      prompt: prompt,
      writer: function writer(obj) {
        return util.inspect(obj, {colors: true, depth: 4});
      }
    });
    //var statements = getStatements(result.tableGraph);
    //var columnInfoByTable = getColumnInfoByTable(result.columnInfo);
    //statements.columns = getCreateTableColumnStatements(columnInfoByTable);
    //var other = {
    //  columnInfoByTable: columnInfoByTable,
    //  statements: statements
    //};
    _.extend(dbRepl.context, {
      ld: _,
      knex: knex,
      bookshelf: schemaInfo.bookshelf,
      schemaInfo: schemaInfo
    }, result
      //, other
    );
    return dbRepl;
  });

}

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

function createRepl(knex, options) {
  var
    opts = options || {},
    driverName = knex.client.driverName,
    databaseName = knex.client.database(),
    SchemaInfo = getSchemaInfo(driverName),
    schemaInfo = new SchemaInfo(knex),
    prompt = opts.prompt || ('[DB] ' + driverName + ' (' + databaseName + ')> '),
    replObjects = {
      metaSchemaModels: schemaInfo.createMetaSchemaModels(),
      schemaModels: schemaInfo.createSchemaModels(databaseName),
      columnInfo: schemaInfo.getColumnInfo(),
      tableInfo: schemaInfo.getTableInfo(),
      viewInfo: schemaInfo.getViewInfo(),
      fullTableInfo: schemaInfo.getFullTableInfo(),
      tableGraph: schemaInfo.getTableGraph(),
      keyColumnDependencies: schemaInfo.getKeyColumnDependencies()
    },
    startOptions = {
      prompt: prompt,
      writer: defaultInspect
    };

  return Promise.props(replObjects).then(function(result) {
    var dbRepl = repl.start(startOptions);
    _.extend(dbRepl.context, {
      ld: _,
      knex: knex,
      bookshelf: schemaInfo.bookshelf,
      db: _.extend({}, {schemaInfo: schemaInfo}, result)
    });
    return dbRepl;
  });

}

function getSchemaInfo(client) {
  return schemaInfos[client];
}

function defaultInspect(obj) {
  return util.inspect(obj, {colors: true, depth: 4});
}

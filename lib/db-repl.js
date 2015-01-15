var
  repl = require('repl'),
  util = require('util'),
  _ = require('lodash'),
  Promise = require('bluebird'),
  sg = require('some-graph'),
  Graph = sg.Graph;

module.exports = {

  start: function start(bookshelf, knex, options) {
    return createRepl(bookshelf, knex, options);
  }

};

function createRepl(bookshelf, knex, options) {
  var DB_CURRENT_SCHEMA = knex.raw('SCHEMA()');

  return startRepl(options);

  function startRepl(options) {
    var
      prompt = options.prompt || '[DB] > ';

    return Promise.props({
      informationSchemaModels: createSchemaModels('information_schema'),
      schema: createSchemaModels(options.database),
      columnInfo: getColumnInfo(),
      tableInfo: getTableInfo(),
      viewInfo: getViewInfo(),
      fullTableInfo: getFullTableInfo(),
      tableGraph: getTableGraph(),
      keyColumnDependencies: getKeyColumnDependencies()
    }).then(function(result) {
      var dbRepl = repl.start({
        prompt: prompt,
        writer: function writer(obj) {
          return util.inspect(obj, {colors: true, depth: 4});
        }
      });
      var statements = getStatements(result.tableGraph);
      var columnInfoByTable = getColumnInfoByTable(result.columnInfo);
      statements.columns = getCreateTableColumnStatements(columnInfoByTable);
      var other = {
        columnInfoByTable: columnInfoByTable,
        statements: statements
      };
      _.extend(dbRepl.context, {
        ld: _,
        knex: knex,
        bookshelf: bookshelf,
        DB_CURRENT_SCHEMA: DB_CURRENT_SCHEMA
      }, result, other);
      return dbRepl;
    });

  }

  function getTableGraph() {
    return Promise.join(
      getTableInfo().select('table_name'),
      getTableDependencies()
    ).spread(function(tables, dependencies) {
        var g = new Graph;
        tables.forEach(function(t) {
          g.addVertex(t.table_name);
        });
        dependencies.forEach(function(row) {
          var
            table = row.table_name,
            referencedTable = row.referenced_table_name,
            from = g.getVertex(table),
            to = g.getVertex(referencedTable);
          g.addEdge(from, to);
        });
        return g;
      });
  }

  function getTableDependencies() {
    return knex('information_schema.referential_constraints').
      select('constraint_name', 'table_name', 'referenced_table_name').
      where('constraint_schema', DB_CURRENT_SCHEMA).
      orderBy('table_name').
      orderBy('referenced_table_name');
  }

  function getKeyColumnDependencies() {
    return knex('information_schema.key_column_usage')
      .where('table_schema', DB_CURRENT_SCHEMA)
      .orderBy('table_name')
      .orderBy('constraint_name')
      .orderBy('ordinal_position');
  }

  function getSchemaTables(schemaName) {
    var column = 'Tables_in_' + schemaName;
    return knex.raw('SHOW FULL TABLES IN ??', [schemaName]).spread(function(rows, sys1, sys2) {
      return _.pluck(rows, column);
    });
  }

  function createSchemaModels(schemaName) {
    return getSchemaTables(schemaName).then(function(tableNames) {
      return createModels(tableNames, schemaName);
    });
  }

  function createModels(tableNames, schemaName) {
    var tables = tableNames || [];
    return _.reduce(tables, function(memo, tableName) {
      var modelName = tableName.toLowerCase();
      memo[modelName] = createModel(tableName, schemaName);
      return memo;
    }, {});
  }

  function createModel(tableName, prefix) {
    return bookshelf.Model.extend({
      tableName: getFullTableName(tableName, prefix)
    });
  }

  function getColumnInfo() {
    return knex('information_schema.columns').
      where('table_schema', DB_CURRENT_SCHEMA).
      orderBy('table_name', 'ordinal_position');
  }

  function getTableInfo() {
    return knex('information_schema.tables').
      where('table_schema', DB_CURRENT_SCHEMA).
      where('table_type', '<>', 'VIEW');
  }

  function getViewInfo() {
    return knex('information_schema.views').where('table_schema', DB_CURRENT_SCHEMA);
  }

  function getFullTableInfo() {
    return getColumnInfo().reduce(function(memo, columnInfo) {
      var table = columnInfo.TABLE_NAME;
      if (memo.hasOwnProperty(memo[table])) {
        memo[table].push(columnInfo);
      }
      else {
        memo[table] = [columnInfo];
      }
      return memo;
    }, {});
  }

  function getCreateTableColumnStatements(columnInfoByTable) {
    return _.reduce(columnInfoByTable, function(memo, columnInfos, table){
      memo[table] = getCreateTableColumnDefinitionStatement(columnInfos);
      return memo;
    }, {});
  }

  function getCreateTableColumnDefinitionStatement(columnInfos) {
    return _.sortBy(columnInfos.map(getColumnDefinition).map(getColumnStatement), 'ordinal');
  }

  function getColumnStatement(c) {
    var
      nullableClause = c.null ? 'NULL' : 'NOT NULL',
      extraClause = !c.extra ? '' : ' ' + c.extra,
      defaultClause = c.default === null && !c.null,
      baseDefinition = c.name + ' ' + c.fullType + ' ' + nullableClause;
    return baseDefinition + ' ' + getDefaultClause(c.type, c.default) + extraClause;
  }

  function getDefaultClause(c) {
    var
      dataType = c.dataType,
      nullable = c.null,
      value = c.default;
    if (!nullable && value === null) return '';
    return value === null ? '' : knex.raw('DEFAULT ?', [getValueForDataType(dataType, value)]).toString();
  }

}

function getFullTableName(tableName, schema) {
  return schema ? schema + '.' + tableName : tableName;
}

/**
 * @param {Graph} graph
 */
function getStatements(graph) {
  var
    sortedNodes = graph.topologicalSort(),
    creationOrder = sortedNodes
      .map(function(v) {
        var vId = v.id;
        return {id: vId, to: graph.out(v)};
      }),
    createStatements = creationOrder
      .map(function(v) {
        return getCreateTableStatement(v.id);
      }, []),
    fkStatements = creationOrder
      .filter(function(v) {
        return v.to.length > 0;
      })
      .reduce(function(memo, v) {
        var
          tableId = v.id,
          to = v.to;
        return memo.concat(to.map(function (v) {
          var
            toTableName = v.id,
            toTableColumn = 'id',
            fkName = toTableName + '_' + toTableColumn;
          return getForeignKeyStatement(tableId, fkName, toTableName, toTableColumn, 'CASCADE', 'CASCADE');
        }));
      }, []),
    dropStatements = creationOrder
      .map(function(v) {
        return getDropTableStatement(v.id, true);
      }).reverse();
  return {
    creates: createStatements,
    drops: dropStatements,
    fks: fkStatements
  };
}

function getForeignKeyStatement(fromTable, fromColumn, toTable, toColumn, deleteRule, updateRule) {
  var
    constraintName = fromTable + '__fk__' + fromColumn,
    del = getFkRule(deleteRule),
    upd = getFkRule(updateRule);
  return 'ALTER TABLE ' + fromTable
    + ' ADD CONSTRAINT ' + constraintName
    + ' FOREIGN KEY (' + fromColumn + ')'
    + ' REFERENCES ' + toTable + ' (' + toColumn + ')'
    + ' ON DELETE ' + del
    + ' ON UPDATE ' + upd;
}

/**
 * @param {String} tableName
 * @param {Boolean} onlyIfExists
 * @return {String}
 */
function getDropTableStatement(tableName, onlyIfExists) {
  return 'DROP TABLE ' + getIfExistsClause(onlyIfExists) + ' ' + tableName + ';';
}

/**
 * @param {Boolean} onlyIfExists
 * @return {String}
 */
function getIfExistsClause(onlyIfExists) {
  return onlyIfExists ? 'IF EXISTS' : '';
}

/**
 * @param {String} rule
 * @return {String}
 */
function getFkRule(rule) {
  return /^(RESTRICT|CASCADE|SET NULL|NO ACTION)$/gi.test(rule) ? rule : 'NO ACTION';
}

function getColumnInfoByTable(columnInfo) {
  return _.groupBy(columnInfo, 'TABLE_NAME');
}

/**
 * @param {String} tableName
 * @return {String}
 */
function getCreateTableStatement(tableName) {
  //todo: add column definitions
  return 'CREATE TABLE ' + tableName + ';';
}

function getColumnDefinition(c) {
  return {
    name: c.COLUMN_NAME,
    fullType: c.COLUMN_TYPE,
    null: c.IS_NULLABLE === 'YES',
    default: c.COLUMN_DEFAULT,
    extra: c.EXTRA,
    ordinal: c.ORDINAL_POSITION,
    type: c.DATA_TYPE
  };
}

function getValueForDataType(dataType, stringValue) {
  return isNumericDataType(dataType) ? parseNumericValue(stringValue)
    : stringValue;
}

function parseNumericValue(stringValue) {
  return +stringValue;
}

function isNumericDataType(dataType) {
  return ['tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'decimal', 'float'].indexOf(dataType) >= 0;
}

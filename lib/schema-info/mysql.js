var
  util = require('util'),
  _ = require('lodash'),
  Promise = require('bluebird'),
  Graph = require('some-graph').Graph,
  SchemaInfoBase = require('./base');

util.inherits(MysqlSchemaInfo, SchemaInfoBase);

module.exports = MysqlSchemaInfo;

/**
 * @param knex
 * @constructor
 */
function MysqlSchemaInfo(knex) {
  SchemaInfoBase.call(this, knex);
  this.DB_CURRENT_SCHEMA = this.knex.raw('SCHEMA()');
}
MysqlSchemaInfo.prototype.getMetaSchemaName = function() {
  return 'information_schema';
};

/**
 * @param {string} schemaName The name of the schema where the table lives. Defaults to the current schema for the connection
 */
MysqlSchemaInfo.prototype.getColumnInfo = function(schemaName) {
  return this.getMetaSchemaTable('columns')
    .where('table_schema', schemaName || this.DB_CURRENT_SCHEMA)
    .orderBy('table_name', 'ordinal_position');
};

MysqlSchemaInfo.prototype.getKeyColumnDependencies = function(schemaName) {
  return this.getMetaSchemaTable('key_column_usage')
    .where('table_schema', schemaName || this.DB_CURRENT_SCHEMA)
    .orderBy('table_name')
    .orderBy('constraint_name')
    .orderBy('ordinal_position');
};

MysqlSchemaInfo.prototype.getTableDependencies = function(schemaName) {
  return this.getMetaSchemaTable('referential_constraints')
    .where('constraint_schema', schemaName || this.DB_CURRENT_SCHEMA)
    .orderBy('table_name')
    .orderBy('referenced_table_name');
};

MysqlSchemaInfo.prototype.getTableInfo = function(schemaName) {
  return this.getMetaSchemaTable('tables').
    where('table_schema', schemaName || this.DB_CURRENT_SCHEMA).
    where('table_type', '<>', 'VIEW');
};

MysqlSchemaInfo.prototype.getViewInfo = function(schemaName) {
  return this.getMetaSchemaTable('views')
    .where('table_schema', schemaName || this.DB_CURRENT_SCHEMA);
};

MysqlSchemaInfo.prototype.getFullTableInfo = function(schemaName) {
  var tableNameColumn = 'TABLE_NAME';
  return this.getColumnInfo(schemaName).reduce(function(memo, columnInfo) {
    var tableName = columnInfo[tableNameColumn];
    if (memo.hasOwnProperty(tableName)) {
      memo[tableName].push(columnInfo);
    }
    else {
      memo[tableName] = [columnInfo];
    }
    return memo;
  }, {});
};

MysqlSchemaInfo.prototype.getCreateTableColumnStatements = function(columnInfoByTable) {
  var self = this;
  return _.reduce(columnInfoByTable, function(memo, columnInfos, table){
    memo[table] = self.getCreateTableColumnDefinitionStatement(columnInfos);
    return memo;
  }, {});
};

MysqlSchemaInfo.prototype.getCreateTableColumnDefinitionStatement = function(columnInfos) {
  var self = this;
  return _.sortBy(columnInfos
    .map(getColumnDefinition)
    .map(self.getColumnStatement.bind(self)), 'ordinal');
};

MysqlSchemaInfo.prototype.getColumnStatement = function(c) {
  var
    self = this,
    nullableClause = c.null ? 'NULL' : 'NOT NULL',
    extraClause = !c.extra ? '' : ' ' + c.extra,
    defaultClause = c.default === null && !c.null,
    baseDefinition = c.name + ' ' + c.fullType + ' ' + nullableClause;
  return baseDefinition + ' ' + self.getDefaultClause(c.type, c.default) + extraClause;
};

MysqlSchemaInfo.prototype.getDefaultClause = function(c) {
  var
    self = this,
    dataType = c.dataType,
    nullable = c.null,
    value = c.default;
  if (!nullable && value === null) return '';
  return value === null ? '' : self.knex.raw('DEFAULT ?', [getValueForDataType(dataType, value)]).toString();
};

MysqlSchemaInfo.prototype.createMetaSchemaModels = function() {
  return this.createSchemaModels(this.getMetaSchemaName());
};

MysqlSchemaInfo.prototype.createSchemaModels = function(schemaName) {
  var self = this;
  return self.getTableInfo(schemaName).then(function(tableRecords) {
    return self.createModels(tableRecords, schemaName);
  });
};

MysqlSchemaInfo.prototype.createModels = function(tableRecords, schemaName) {
  var
    self = this,
    tableNameField = 'TABLE_NAME',
    tables = tableRecords || [];
  return _.reduce(tables, function(memo, tableRecord) {
    var
      tableName = tableRecord[tableNameField],
      modelName = tableName.toLowerCase();
    memo[modelName] = self.createModel(tableName, schemaName);
    return memo;
  }, {});
};

MysqlSchemaInfo.prototype.createModel = function(tableName, prefix) {
  return this.bookshelf.Model.extend({
    tableName: getFullTableName(tableName, prefix)
  });
};

MysqlSchemaInfo.prototype.getTableGraph = function() {
  var
    tableNameField = 'TABLE_NAME',
    referencedTableNameField = 'REFERENCED_TABLE_NAME';
  return Promise.join(
    this.getTableInfo(),
    this.getTableDependencies()
  ).spread(function(tables, dependencies) {
      var g = new Graph;
      tables.forEach(function(row) {
        g.addVertex(row[tableNameField]);
      });
      dependencies.forEach(function(row) {
        var
          table = row[tableNameField],
          referencedTable = row[referencedTableNameField],
          from = g.getVertex(table),
          to = g.getVertex(referencedTable);
        g.addEdge(from, to);
      });
      return g;
    });
};

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

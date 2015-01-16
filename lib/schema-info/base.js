var
  Bookshelf = require('bookshelf'),
  empty = function(){};

/**
 * @type {SchemaInfoBase}
 */
module.exports = SchemaInfoBase;

/**
 * @param {Knex} knex An instance of the knex query builder
 * @constructor
 */
function SchemaInfoBase(knex) {
  this.knex = knex;
  this.bookshelf = Bookshelf.initialize(knex);
}
SchemaInfoBase.prototype = {
  getColumnInfo: empty,
  getKeyColumnDependencies: empty,
  getFullTableInfo: empty,
  getTableDependencies: empty,
  getTableInfo: empty,
  getViewInfo: empty,
  getCreateTableColumnDefinitionStatement: empty,
  getColumnStatement: empty,
  getDefaultClause: empty,
  getTableGraph: empty,

  createSchemaModels: empty,
  createModels: empty,
  createModel: empty,
  createMetaSchemaModels: empty
};
SchemaInfoBase.prototype.getMetaSchemaName = function() {
  return 'information_schema';
};

SchemaInfoBase.prototype.getMetaSchemaTableName = function(tableName) {
  return this.getMetaSchemaName() + '.' + tableName;
};

SchemaInfoBase.prototype.getMetaSchemaTable = function(tableName) {
  return this.knex(this.getMetaSchemaTableName(tableName));
};

'use strict';

angular.module('cosyan')
.service('util', function($http) {
  this.createNewEntity = function(entityType) { 
    if (!entityType) {
      return;
    }
    var entity = { type: entityType.name, fields: [], foreignKeys: [] };
    for (var i in entityType.fields) {
      var field = entityType.fields[i];
      entity.fields.push({ name: field.name, type: field.type, value: undefined });
    }
    for (var i in entityType.foreignKeys) {
      var fk = entityType.foreignKeys[i];
      entity.foreignKeys.push({
        name: fk.name,
        type: fk.type,
        refTable: fk.refTable,
        columnName: fk.column,
        value: undefined });
    }
    return entity;
  };
  
  this.format = function(field) {
	if (field.value === undefined) {
      return 'null';
    }
	if (field.type.type === 'timestamp') {
	  return 'dt \'' + field.value + '\'';
	} else if (field.type.type === 'varchar' || field.type.type === 'enum' || isNaN(field.value)) {
      return '\'' + field.value + '\'';
    } else {
      return field.value;
    }
  };
  
  this.meta = function(s, e, type) {
	$http.get("/cosyan/entityMeta", {
	  params: { token: sessionStorage.getItem('token') }
	}).then(function success(response) {
      var meta = response.data.entities.find(function(entity) {
        return entity.name === type;
      });
	  s(meta);
	}, function error(response) {
	  e(response.data.error);
	});  
  };
});
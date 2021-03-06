'use strict';

angular.module('cosyan').directive('entityEditor', ['$http', 'util', function($http, util) {
  return {
    restrict: 'E',
    scope: {
      entity: '=',
      reload: '&',
      setparent: '&',
    },
    templateUrl: 'entity/entity-editor.html',
    link: function(scope, element) {
      scope.dirty = false;
      scope.entityList = {};
      scope.entityPick = undefined;
      scope.meta = undefined;
      util.meta(function success(meta) {
    	scope.meta = meta;
      }, function error(error) {
        scope.$error = error;
      }, scope.entity.type);
      
      scope.isNew = function() {
    	return scope.entity.fields[0].value != undefined;
      };
      
      scope.saveEntity = function() {
    	if (!scope.dirty) {
    	  return;
    	}
        var table = scope.entity.type;
        var query;
        if (scope.isNew()) {
          // Has primary key ID, update statement.
          query = 'update ' + table + ' set ';
          var values = [];
          for (var i = 1; i < scope.entity.fields.length; i++) { 
        	var field = scope.entity.fields[i];
            if (!field.immutable) {
        	  values.push(field.name + ' = ' + util.format(field));
            }
          }
          for (var i = 0; i < scope.entity.foreignKeys.length; i++) { 
            var fk = scope.entity.foreignKeys[i];
            if (!fk.immutable) {
              values.push(fk.columnName + ' = ' + util.format(fk));
            }
          }
          query = query + values.join(', ');
          var idField = scope.entity.fields[0];
          query = query + ' where ' + idField.name + ' = ' + idField.value + ';';
        } else {
          // Has no primary key ID, insert statement.
          query = 'insert into ' + table + ' (';
          var names = [];
          var values = [];
          for (var i = 1; i < scope.entity.fields.length; i++) { 
            var field = scope.entity.fields[i];
            names.push(field.name);
            values.push(util.format(field));
          }
          for (var i = 0; i < scope.entity.foreignKeys.length; i++) { 
            var fk = scope.entity.foreignKeys[i];
            names.push(fk.columnName);
            values.push(util.format(fk)); 
          }
          query = query + names.join(', ') + ') values (' + values.join(', ') + ');';
        }
        
        $http.get("/cosyan/sql", {
          params: { sql: query, token: sessionStorage.getItem('token') }
        }).then(function success(response) {
          scope.dirty = false;
          var newIDs = response.data.result[0].newIDs;
          if (newIDs) {
        	scope.entity.fields[0].value = newIDs[0];
        	scope.setparent({newID: newIDs[0]});
          }
          scope.reload();
        }, function error(response) {
          scope.$error = response.data.error;
        });
      };
      
      scope.discardEntity = function() {
        scope.reload();
      };
      
      scope.valueChange = function() {
        scope.dirty = true;
      };
      
      scope.expandEntity = function(fk) {
    	if (scope.entityList[fk.name] || fk.value == undefined) {
          scope.entityList[fk.name] = undefined;
          return;
        }
        $http.get("/cosyan/loadEntity", {
          params: { table: fk.refTable, id: fk.value, token: sessionStorage.getItem('token') }
        }).then(function success(response) {
          scope.entityList[fk.name] = response.data.result[0];
        }, function error(response) {
          scope.$error = response.data.error;
        });
      };
      
      scope.expandEntityList = function(rfk) {
        if (scope.entityList[rfk.name]) {
          scope.entityList[rfk.name] = undefined;
          return;
        }
        var query = 'select * from ' + rfk.refTable + ' where ' + rfk.refColumn + ' = ' + scope.entity.fields[0].value + ';';
        $http.get("/cosyan/sql", {
          params: { sql: query, token: sessionStorage.getItem('token') }
        }).then(function success(response) {
          scope.entityList[rfk.name] = response.data.result[0];
        }, function error(response) {
          scope.$error = response.data.error;
        });
      };
      
      scope.expandAggref = function(aggref) {
    	if (scope.entityList[aggref.name]) {
          scope.entityList[aggref.name] = undefined;
          return;
        }
    	var cols = aggref.columns.map(c => aggref.name + '.' + c).join(', ');
        var query = 'select ' + cols + ' from ' + scope.entity.type + ' where ' + scope.entity.fields[0].name + ' = ' + scope.entity.fields[0].value + ';';
        $http.get("/cosyan/sql", {
          params: { sql: query, token: sessionStorage.getItem('token') }
        }).then(function success(response) {
          var entity = { type: aggref.name, fields: [] };
          var r = response.data.result[0];
          for (var i in r.header) {
            entity.fields.push({
              name: r.header[i],
              type: r.types[i],
              value: r.values[0][i]
            });
          }
          scope.entityList[aggref.name] = entity;
        }, function error(response) {
          scope.$error = response.data.error;
        });
      };
      
      scope.pickEntity = function(fk) {
        if(!scope.entityPick && !fk.immutable) {
          scope.entityPick = fk.name;
        } else {
          scope.entityPick = undefined;
        }
      };
      
      scope.unsetEntity = function(fk) {
        fk.value = undefined;
        scope.entityList[fk.name] = undefined;
        scope.dirty = true;
      };
      
      scope.unsetValue = function(field) {
    	field.value = undefined;
        scope.dirty = true;
      };
      
      scope.pick = function(fk, id) {
        scope.entityPick = undefined;
        scope.entityList[fk.name] = undefined;
        fk.value = id;
        scope.expandEntity(fk);
        scope.dirty = true;
      };
      
      scope.cancelPick = function(fk) {
        scope.entityPick = undefined;
      };
      
      scope.setNewFK = function(fk, newID) {
    	fk.value = newID;
    	scope.dirty = true;
      };
      
      scope.newEntity = function(fk) {
    	if (!fk.immutable) {
    	  util.meta(function success(meta) {
    	    scope.entityList[fk.name] = util.createNewEntity(meta);
    	  }, function error(error) {
            scope.$error = error;
          }, fk.refTable);
    	}
      };
      
      scope.$watch('entity', function(newVal, oldVal){
    	scope.dirty = false;
        scope.entityList = {};
        scope.entityPick = undefined;
      });
    },
  };
}]);
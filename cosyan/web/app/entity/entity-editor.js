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
      scope.entityPick = {};
      
      scope.format = function(field) {
        if (field.value === undefined) {
          return 'null';
        }
        if (field.type.type === 'varchar' || field.type.type === 'enum') {
          return '\'' + field.value + '\'';
        } else {
          return field.value;
        }
      }
      
      scope.saveEntity = function() {
    	if (!scope.dirty) {
    	  return;
    	}
        var table = scope.entity.type;
        var query;
        if (scope.entity.fields[0].value) {
          // Has primary key ID, update statement.
          query = 'update ' + table + ' set ';
          var values = [];
          for (var i = 1; i < scope.entity.fields.length; i++) { 
            var field = scope.entity.fields[i];
            values.push(field.name + ' = ' + scope.format(field)); 
          }
          for (var i = 0; i < scope.entity.foreignKeys.length; i++) { 
            var fk = scope.entity.foreignKeys[i];
            values.push(fk.columnName + ' = ' + scope.format(fk)); 
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
            values.push(scope.format(field));
          }
          for (var i = 0; i < scope.entity.foreignKeys.length; i++) { 
            var fk = scope.entity.foreignKeys[i];
            names.push(fk.columnName);
            values.push(scope.format(fk)); 
          }
          query = query + names.join(', ') + ') values (' + values.join(', ') + ');';
        }
        
        $http.get("/cosyan/sql", {
          params: { sql: query }
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
        if (scope.entityList[fk.name] || !fk.value) {
          scope.entityList[fk.name] = undefined;
          return;
        }
        $http.get("/cosyan/loadEntity", {
          params: { table: fk.refTable, id: fk.value }
        }).then(function success(response) {
          scope.entityList[fk.name] = response.data.result[0];
        }, function error(response) {
          $scope.$error = response.data.error;
        });
      };
      
      scope.expandEntityList = function(rfk) {
        if (scope.entityList[rfk.name]) {
          scope.entityList[rfk.name] = undefined;
          return;
        }
        var query = 'select * from ' + rfk.refTable + ' where ' + rfk.refColumn + ' = ' + scope.entity.fields[0].value + ';';
        $http.get("/cosyan/sql", {
          params: { sql: query }
        }).then(function success(response) {
          scope.entityList[rfk.name] = response.data.result[0];
        }, function error(response) {
          scope.$error = response.data.error;
        });
      };
      
      scope.pickEntity = function(fk) {
        if(!scope.entityPick[fk.name]) {
          scope.entityPick[fk.name] = 1;
        } else {
          scope.entityPick[fk.name] = undefined;
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
        scope.entityPick[fk.name] = undefined;
        scope.entityList[fk.name] = undefined;
        fk.value = id;
        scope.expandEntity(fk);
        scope.dirty = true;
      };
      
      scope.cancelPick = function(fk) {
        scope.entityPick[fk.name] = undefined;
      };
      
      scope.setNewFK = function(fk, newID) {
    	fk.value = newID;
    	scope.dirty = true;
      };
      
      scope.newEntity = function(fk) {
    	var entityType;
    	$http.get("/cosyan/entityMeta").then(function success(response) {
    	  entityType = response.data.entities.find(function(entity) {
    	    return entity.name === fk.refTable;
    	  });
    	  
    	  scope.entityList[fk.name] = util.createNewEntity(entityType);
    	});
      };
    },
  };
}]);
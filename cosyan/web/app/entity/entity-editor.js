'use strict';

angular.module('cosyan').directive('entityEditor', ['$http', function($http) {
  return {
    restrict: 'E',
    scope: {
      entity: '=',
      reload: '&',
    },
    templateUrl: 'entity/entity-editor.html',
    link: function(scope, element) {
      scope.dirty = false;
      
      scope.format = function(field) {
        if (field.type === 'varchar') {
          return '\'' + field.value + '\'';
        } else {
          return field.value;
        }
      }
      
      scope.saveEntity = function() {
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
          query = query + values.join(', ');
          var idField = scope.entity.fields[0];
          query = query + ' where ' + idField.name + ' = ' + idField.value + ';';
        } else {
          // Has no primary key ID, insert statement.
          query = 'insert into ' + table + ' values (';
          var values = [];
          for (var i = 1; i < scope.entity.fields.length; i++) { 
            var field = scope.entity.fields[i];
            values.push(scope.format(field));
          }
          query = query + values.join(', ') + ');';
        }
        
        $http.get("/cosyan/sql", {
          params: { sql: query }
        }).then(function success(response) {
          scope.dirty = false;
          scope.reload();
        }, function error(response) {
          scope.reload();
        });
      };
    },
  };
}]);
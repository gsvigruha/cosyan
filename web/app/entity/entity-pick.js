'use strict';

angular.module('cosyan').directive('entityPick', ['$http', 'util', function($http, util) {
  return {
    restrict: 'E',
    scope: {
      type: '=',
      pick: '&',
      cancel: '&',
    },
    templateUrl: 'entity/entity-pick.html',
    link: function(scope, element) {
      scope.loadMeta = function() {
        util.meta(function success(meta) {
          scope.meta = meta;
          for (var i in scope.meta.fields) {
        	var field = scope.meta.fields[i];
        	if (field.search) {
        	  scope.searchFields[field.name] = { value: undefined, type: field.type };
            }
          }
        }, function error(error) {
          $scope.$error = error;
          scope.meta = undefined;
        }, scope.type);
      };
      
      scope.searchFields = {};
      scope.searchEntity = function() {
    	var query = 'select * from ' + scope.meta.name;
    	var where = [];
        for (var field in scope.searchFields) {
    	  if (scope.searchFields[field].value) {
    	    where.push(field + '=' + util.format(scope.searchFields[field]));
    	  }
    	}
    	if (where.length > 0) {
    	  query = query + ' where ' + where.join(' and ');
        }
    	query = query + ';';
    	$http.get("/cosyan/sql", {
    	  params: { sql: query, token: sessionStorage.getItem('token') }
        }).then(function success(response) {
    	  scope.entityList = response.data.result[0];
          scope.$error = undefined;
        }, function error(response) {
          scope.entityList = undefined;
          scope.$error = response.data.error;
        });
      };
      
      scope.loadMeta();
    },
  };
}]);
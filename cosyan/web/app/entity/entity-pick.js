'use strict';

angular.module('cosyan').directive('entityPick', ['$http', function($http) {
  return {
    restrict: 'E',
    scope: {
      type: '=',
      pick: '&',
    },
    templateUrl: 'entity/entity-pick.html',
    link: function(scope, element) {
      scope.loadMeta = function() {
        $http.get("/cosyan/entityMeta").then(function success(response) {
          scope.meta = response.data.entities.find(function(entity) {
            return entity.name === scope.type;
          });
        }, function error(response) {
          scope.meta = undefined;
        });
      };
      
      scope.searchFields = {};
      scope.searchEntity = function() {
        var params = { table: scope.type };
        for (var field in scope.searchFields) {
          params['filter_' + field] = scope.searchFields[field];
        }
        $http.get("/cosyan/searchEntity", {
          params: params
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
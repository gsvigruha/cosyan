'use strict';

angular.module('cosyan').directive('entityList', ['$http', 'util', function($http, util) {
  return {
    restrict: 'E',
    scope: {
      entities: '=',
      open: '&?',
      delete: '&?',
      pick: '&?',
      type: '=',
    },
    templateUrl: 'entity/entity-list.html',
    link: function(scope, element) {
      util.meta(function success(meta) {
        scope.meta = meta;
      }, function error(error) {
        scope.$error = error;
      }, scope.type);    
    },
  };
}]);
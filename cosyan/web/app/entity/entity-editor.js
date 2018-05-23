'use strict';

angular.module('cosyan').directive('entityEditor', ['$http', function($http) {
  return {
    restrict: 'E',
    scope: {
      entity: '=',
    },
    templateUrl: 'entity/entity-editor.html',
    link: function(scope, element) {
    },
  };
}]);
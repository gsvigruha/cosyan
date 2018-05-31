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
      scope.saveEntity = function() {
        var params = { 'table': scope.entity.type };
        for (var i = 0; i < scope.entity.fields.length; i++) { 
          var field = scope.entity.fields[i];
          if (field.name === scope.entity.pk) {
            params['id_name'] = scope.entity.pk;
            params['id_value'] = field.value;
          } else {
            params['param_' + field.name] = field.value;
          }
        }
        $http.get("/cosyan/saveEntity", {
          params: params
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
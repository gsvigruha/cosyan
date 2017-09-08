'use strict';

angular.module('cosyan')
.controller('SQLCtrl', function($scope, $http) {
  $scope.run = function() {
    $http.get("/cosyan/sql", {
      params: { sql: $scope.query }
    }).then(function(response) {
      $scope.data = response.data;
    });
  };
});
'use strict';

angular.module('cosyan')
.controller('SQLCtrl', function($scope, $http) {
  $scope.run = function() {
    $http.get("/cosyan/sql", {
      params: { sql: $scope.query, user: sessionStorage.getItem('user') }
    }).then(function success(response) {
      $scope.data = response.data;
      $scope.$error = undefined;
    }, function error(response) {
      $scope.data = undefined;
      $scope.$error = response.data.error;
    });
  };
});
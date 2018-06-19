'use strict';

angular.module('cosyan')
.controller('SQLCtrl', function($scope, $http) {
  $scope.run = function() {
    var user = sessionStorage.getItem('user');
    $http.get("/cosyan/sql", {
      params: { sql: $scope.query, user: user }
    }).then(function success(response) {
      $scope.data = response.data;
      $scope.$error = undefined;
    }, function error(response) {
      $scope.data = undefined;
      $scope.$error = response.data.error;
    });
  };
});
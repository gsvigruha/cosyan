'use strict';

angular.module('cosyan')
.controller('AdminCtrl', function($scope, $http) {
  $scope.loadTables = function() {
    $http.get("/cosyan/admin").then(function(response) {
      $scope.data = response.data;
    });
  };
  $scope.loadTables();
});
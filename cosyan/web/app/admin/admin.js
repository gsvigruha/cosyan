'use strict';

angular.module('cosyan')
.controller('AdminCtrl', function($scope, $http, $document) {
  $scope.tableOpened = {};
  
  $scope.pickTable = function(tableName) {
    $scope.activeTable = $scope.data.tables[tableName];
  };
  
  $scope.tableLoc = {};
  
  $scope.loadTables = function() {
    $http.get("/cosyan/admin", {
        params: { user: sessionStorage.getItem('user') }
    }).then(function(response) {
      $scope.data = response.data;
    });
  };
  $scope.loadTables();
});
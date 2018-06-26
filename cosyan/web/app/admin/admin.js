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
      if ($scope.activeTable) {
        $scope.pickTable($scope.activeTable.name);
      }
    });
  };
  
  $scope.dropConstraint = function(name) {
    var query = 'alter table ' + $scope.activeTable.name + ' drop constraint ' + name + ';';
	$http.get("/cosyan/sql", {
	  params: { sql: query, user: sessionStorage.getItem('user') }
	}).then(function success(response) {
      $scope.$error = undefined;
      $scope.loadTables();
    }, function error(response) {
      $scope.$error = response.data.error;
    });
  };
  
  $scope.dropColumn = function(name) {
    var query = 'alter table ' + $scope.activeTable.name + ' drop ' + name + ';';
	$http.get("/cosyan/sql", {
	  params: { sql: query, user: sessionStorage.getItem('user') }
	}).then(function success(response) {
	  $scope.$error = undefined;
	  $scope.loadTables();
    }, function error(response) {
	  $scope.$error = response.data.error;
    });
  };
  
  $scope.loadTables();
});
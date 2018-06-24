'use strict';

angular.module('cosyan')
.controller('AdminCtrl', function($scope, $http, $document) {
  $scope.tableOpened = {};
  
  $scope.expandTable = function(table) {
    if ($scope.tableOpened[table.name]) {
      $scope.tableOpened[table.name] = 0;
    } else {
      $scope.tableOpened[table.name] = 1;
    }
  };
  
  $scope.tableLoc = {};
  
  $scope.loadTables = function() {
    $http.get("/cosyan/admin", {
        params: { user: sessionStorage.getItem('user') }
    }).then(function(response) {
      $scope.data = response.data;
      for (var i in $scope.data.tables) {
        var table = $scope.data.tables[i];
        var loc = {x: Math.floor(i / 3) * 320 + 100, y: (i % 3) * 250 + 100 };
        $scope.tableLoc[table.name] = loc;
      }
    });
  };
  $scope.loadTables();
  
  $scope.tableStyle = function(table, i) {
	return { 'top':  '' + $scope.y(table.name) + 'px', 'left': '' + $scope.x(table.name) + 'px' };
  };
  
  $scope.x = function(table) {
	return $scope.tableLoc[table].x;
  };
  $scope.y = function(table) {
    return $scope.tableLoc[table].y;
  };
});
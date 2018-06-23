'use strict';

angular.module('cosyan')
.controller('AdminCtrl', function($scope, $http, $document) {
  $scope.loadTables = function() {
    $http.get("/cosyan/admin", {
        params: { user: sessionStorage.getItem('user') }
    }).then(function(response) {
      $scope.data = response.data;
    });
  };
  $scope.loadTables();
  
  $scope.tableStyle = function(table, i) {
	var y = (i % 3) * 250 + 100;
	var x = Math.floor(i / 3) * 320 + 100;
	return { 'top':  '' + y + 'px', 'left': '' + x + 'px' };
  };
});
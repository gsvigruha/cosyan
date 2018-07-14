'use strict';

angular.module('cosyan')
.controller('MonitoringCtrl', function($scope, $http) {
  $scope.loadMonitoring = function() {
    $http.get("/cosyan/monitoring", {
        params: { user: sessionStorage.getItem('user') }
    }).then(function(response) {
      $scope.data = response.data;
    });
  };
  
  $scope.testKeys = {};
  $scope.testValues = {};
  
  $scope.loadIndexValues = function(indexName) {
	$http.get("/cosyan/index", {
	  params: { user: sessionStorage.getItem('user'), index: indexName, key: $scope.testKeys[indexName] }
	}).then(function success(response) {
	  $scope.testValues[indexName] = response.data.pointers;
	}, function error(response) {
      $scope.testValues[indexName] = response.data.error.msg;
	});
  };
  
  $scope.loadMonitoring();
});
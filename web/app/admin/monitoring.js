'use strict';

angular.module('cosyan')
.controller('MonitoringCtrl', function($scope, $http) {
  $scope.loadMonitoring = function() {
    $http.get("/cosyan/monitoring", {
        params: { token: sessionStorage.getItem('token') }
    }).then(function(response) {
      $scope.data = response.data;
    });
  };
  
  $scope.testKeys = {};
  $scope.testValues = {};
  
  $scope.loadIndexValues = function(indexName) {
	$http.get("/cosyan/index", {
	  params: { token: sessionStorage.getItem('token'), index: indexName, key: $scope.testKeys[indexName] }
	}).then(function success(response) {
	  $scope.testValues[indexName] = response.data.pointers;
	}, function error(response) {
      $scope.testValues[indexName] = response.data.error.msg;
	});
  };
  
  $scope.loadMonitoring();
});
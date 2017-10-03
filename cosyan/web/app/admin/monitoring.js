'use strict';

angular.module('cosyan')
.controller('MonitoringCtrl', function($scope, $http) {
  $scope.loadMonitoring = function() {
    $http.get("/cosyan/monitoring").then(function(response) {
      $scope.data = response.data;
    });
  };
  $scope.loadMonitoring();
});
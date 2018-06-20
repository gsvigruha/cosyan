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
  $scope.loadMonitoring();
});
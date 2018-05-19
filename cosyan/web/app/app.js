'use strict';

// Declare app level module which depends on views, and components
angular.module('cosyan', [
  'ngRoute',
])
.config(function ($routeProvider, $locationProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'sql/sql.html',
        controller: 'SQLCtrl',
      })
      .when('/sql', {
        templateUrl: 'sql/sql.html',
        controller: 'SQLCtrl',
      })
      .when('/admin', {
        templateUrl: 'admin/admin.html',
        controller: 'AdminCtrl',
      })
      .when('/monitoring', {
        templateUrl: 'admin/monitoring.html',
        controller: 'MonitoringCtrl',
      })
      .when('/entity', {
        templateUrl: 'entity/entity.html',
        controller: 'EntityCtrl',
      });
});
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
      .when('/meta', {
        templateUrl: 'admin/meta.html',
        controller: 'MetaCtrl',
      })
      .when('/users', {
        templateUrl: 'admin/users.html',
        controller: 'UsersCtrl',
      })
      .when('/monitoring', {
        templateUrl: 'admin/monitoring.html',
        controller: 'MonitoringCtrl',
      })
      .when('/login', {
        templateUrl: 'admin/login.html',
        controller: 'LoginCtrl',
      })
      .when('/entity', {
        templateUrl: 'entity/entity-main.html',
        controller: 'EntityCtrl',
      })
      .when('/help', {
        templateUrl: 'help/help.html',
        controller: 'HelpCtrl',
      });
});
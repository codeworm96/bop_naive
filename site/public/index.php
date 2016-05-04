<?php
use \Psr\Http\Message\ServerRequestInterface as Request;
use \Psr\Http\Message\ResponseInterface as Response;

require '../vendor/autoload.php';

$app = new \Slim\App;
$app->any('/', function (Request $request, Response $response) {
    // Fetch the array from request
    $data = $request->getParsedBody();

    // Process Data......

    $result = [];   // Empty array

    // Return processed result
    return $response->withJson($result);
});
$app->run();
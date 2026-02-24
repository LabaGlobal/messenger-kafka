<?php

declare(strict_types=1);

use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;
use Koco\Kafka\RdKafka\RdKafkaFactory;
use Koco\Kafka\Messenger\KafkaTransportFactory;
use Koco\Kafka\Messenger\RestProxyTransportFactory;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\UriFactoryInterface;

return static function (ContainerConfigurator $container): void
{
    $services = $container->services();

    $services->set(RdKafkaFactory::class)
        ->public(false)
    ;

    $services->set(KafkaTransportFactory::class)
        ->public(false)
        ->tag('messenger.transport_factory')
        ->args([
            new Reference(RdKafkaFactory::class),
            new Reference('logger', ContainerInterface::NULL_ON_INVALID_REFERENCE),
        ])
    ;

    $services->set(RestProxyTransportFactory::class)
        ->public(false)
        ->tag('messenger.transport_factory')
        ->args([
            new Reference('logger', ContainerInterface::NULL_ON_INVALID_REFERENCE),
            new Reference(ClientInterface::class, ContainerInterface::NULL_ON_INVALID_REFERENCE),
            new Reference(RequestFactoryInterface::class, ContainerInterface::NULL_ON_INVALID_REFERENCE),
            new Reference(UriFactoryInterface::class, ContainerInterface::NULL_ON_INVALID_REFERENCE),
            new Reference(StreamFactoryInterface::class, ContainerInterface::NULL_ON_INVALID_REFERENCE),
        ]);
};

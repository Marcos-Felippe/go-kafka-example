package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {

	// Configuração do Producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Criação do Producer
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalln("Erro ao criar o Producer:", err)
	}

	defer producer.Close()

	// Criação das mensagens
	go func() {
		for i := 0; i <= 10; i++ {
			message := fmt.Sprintf("- Mensagem %d", i)
			producer.Input() <- &sarama.ProducerMessage{
				Topic: "myTopic",
				Value: sarama.StringEncoder(message),
			}

			fmt.Printf("Mensagem %s enviada com sucesso\n", message)
		}

	}()

	consumer()
}

func consumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Adicione aqui as configurações do seu servidor Kafka
	brokers := []string{"localhost:9094"}
	topic := "myTopic"

	// Crie um consumidor Kafka
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalln("Erro ao criar consumidor Kafka:", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Erro ao fechar consumidor Kafka:", err)
		}
	}()

	// Configure um canal de sinais para capturar as interrupções do usuário
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Crie um canal para receber mensagens do Kafka
	messages := make(chan *sarama.ConsumerMessage, 256)

	// Inicie a leitura da fila do Kafka
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln("Erro ao obter partições do Kafka:", err)
	}

	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalln("Erro ao criar consumidor de partição Kafka:", err)
		}
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln("Erro ao fechar consumidor de partição Kafka:", err)
			}
		}()

		go func() {
			for message := range partitionConsumer.Messages() {
				messages <- message
			}
		}()
	}

	// Aguarde mensagens do Kafka e imprima seu conteúdo
	for {
		select {
		case message := <-messages:
			fmt.Printf("Mensagem recebida: %s\n", string(message.Value))
		case <-signals:
			fmt.Println("Interrompendo leitura da fila do Kafka...")
			return
		}
	}
}

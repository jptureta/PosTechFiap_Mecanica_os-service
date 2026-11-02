Feature: Fluxo distribuído de Ordem de Serviço com Saga Orquestrada via SQS
  Como cliente e equipe operacional da oficina mecânica
  Desejo que a abertura, orçamento, aprovação, pagamento, reserva de estoque, execução e entrega de uma OS sejam coordenados de forma transacional
  Para garantir consistência eventual e compensação automática em caso de falhas

  Scenario: Fluxo completo de uma OS até a entrega com diagnóstico NoSQL
    Given o catálogo possui a peça "filtro-oleo" ao preço "45.00" e o serviço "troca-oleo" ao preço "80.00"
    When o cliente "cli-10" abre uma OS para o veículo "ABC-1234" com 1 peça "filtro-oleo" e 1 serviço "troca-oleo"
    And o relay da outbox publica os comandos pendentes no SQS
    And o Billing processa a criação do orçamento e emite o evento QuoteCreated
    And o cliente aprova o orçamento gerando o evento QuoteApproved
    And o pagamento "pay-99" é confirmado gerando o evento PaymentConfirmed
    And o Inventory reserva as peças gerando o evento StockReserved
    Then a OS deve estar no status "READY_FOR_SERVICE" e na fila de execução
    When o mecânico "Roberto" inicia o diagnóstico e registra o código OBD "P0420" no DynamoDB
    And o mecânico avança para reparo, conclui a execução e realiza a entrega do veículo
    Then a OS deve estar no status "DELIVERED" e possuir 1 laudo técnico no DynamoDB

  Scenario: Falha de reserva de estoque após pagamento aciona compensação e reembolso
    Given o catálogo possui a peça "filtro-oleo" ao preço "45.00" e o serviço "troca-oleo" ao preço "80.00"
    When o cliente "cli-10" abre uma OS para o veículo "XYZ-9999" com 2 peças "filtro-oleo"
    And o orçamento é criado, aprovado e o pagamento "pay-err" é confirmado
    And o Inventory informa falha de estoque gerando o evento StockReservationFailed
    And o Billing processa a compensação gerando o evento PaymentRefundRequested
    Then a OS deve estar no status "CANCELLED" com estado de Saga "COMPENSATED"


import pytest
import time
from src.testing.verifier import CSVVerifier

# Этот тест описывает ТВОЙ сценарий.
# Он работает ТОЛЬКО с кодом и существующими CSV.

def test_lost_sale_step_1_funding(sc_client, mc_client):
    """
    Сценарий: Проверка Step 1 (Funding Branch).
    Цель: Убедиться, что записи с Reason='Failed finance' попали в Funding DE.
    """
    from src.data_factory import Auto360DataFactory
    
    # ---------------------------------------------------------
    # 1. ДАННЫЕ (DATA)
    # Где описываем? -> В папке data/lost_sale_cnst/
    # Что важно? -> В Opportunity.csv у записи "Funding" должен быть _BaseName=Opp_Funding
    # ---------------------------------------------------------
    
    print("\n--- [Step 1] Создаем данные в Salesforce ---")
    factory = Auto360DataFactory(sc_client)
    
    # Запускает ВСЕ csv из папки (Account, Opp, Contact...)
    factory.run_scenario("data/lost_sale_cnst")
    
    # Теперь у фабрики есть словарь:
    # factory.key_map['Opp_Funding'] = '006Dn0000...'
    # factory.key_map['Opp_Excluded'] = '006Dn0000...'

    # ---------------------------------------------------------
    # 2. СЦЕНАРИЙ (ORCHESTRATION)
    # Где описываем? -> Прямо тут, в коде.
    # Мы говорим системе, что делать, шаг за шагом.
    # ---------------------------------------------------------

    print("\n--- [Step 2] Запускаем процессы Marketing Cloud ---")
    
    # Эмуляция ожидания синхронизации (реально 15 мин)
    # time.sleep(15 * 60) 

    # Запускаем сбор консентов (как ты писал в требованиях)
    mc_client.run_automation("Auto360_ConsentCollection_MBU_NO_ENT")
    
    # Запускаем основной путь (Journey Entry)
    mc_client.run_automation("Auto360_Lost_Sale_Journey_Flows_with_Consents")

    # ---------------------------------------------------------
    # 3. ПРОВЕРКА (VERIFICATION)
    # Где описываем правила? -> В коде ниже.
    # Мы используем ключи (_BaseName) из CSV.
    # ---------------------------------------------------------
    print("\n--- [Step 3] Проверяем Data Extensions ---")

    # Целевая DE из требований
    TARGET_DE = "Auto360_Lost_Sale_Funding_Engagement"
    DE_KEY_COL = "OpportunityId" # Колонка в DE, где лежит ID Оппо
    
    # А. Проверка Happy Path (Запись ДОЛЖНА быть)
    funding_opp_id = factory.key_map.get('Opp_Funding') # Берем ID, который создался 5 минут назад
    
    if funding_opp_id:
        rows = mc_client.fetch_de_rows(TARGET_DE, DE_KEY_COL, funding_opp_id)
        assert len(rows) > 0, f"Ошибка! Оппортюнити {funding_opp_id} (Opp_Funding) НЕ попало в {TARGET_DE}"
        print(f"✅ Успех: Opp_Funding найдено в {TARGET_DE}")
        
        # Доп. проверка полей внутри строки
        row_data = rows[0]
        assert row_data['EmailAddress'] is not None, "Ошибка! Email пустой!"
    else:
        pytest.fail("Критическая ошибка: Opp_Funding даже не создалось в Salesforce!")

    # Б. Проверка Excluded (Запись НЕ ДОЛЖНА быть)
    excluded_opp_id = factory.key_map.get('Opp_Excluded')
    
    if excluded_opp_id:
        rows = mc_client.fetch_de_rows(TARGET_DE, DE_KEY_COL, excluded_opp_id)
        assert len(rows) == 0, f"Ошибка! Excluded Opp {excluded_opp_id} попало в {TARGET_DE}, а не должно!"
        print(f"✅ Успех: Opp_Excluded правильно отфильтровано (нет в {TARGET_DE})")

    print("\n🏁 Тест завершен успешно.")

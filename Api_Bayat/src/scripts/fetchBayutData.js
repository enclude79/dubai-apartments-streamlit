const https = require('https');
const fs = require('fs');

const options = {
  hostname: 'bayut.p.rapidapi.com',
  path: '/properties/list/?locationExternalIDs=5002,6020',
  method: 'GET',
  headers: {
    'X-RapidAPI-Key': 'bd9812d039mshaf648f73f6fe561p1d9206jsnc85ea1d15d4e',
    'X-RapidAPI-Host': 'bayut.p.rapidapi.com'
  }
};

const req = https.request(options, (res) => {
  let data = '';

  res.on('data', (chunk) => {
    data += chunk;
  });

  res.on('end', () => {
    try {
      const jsonData = JSON.parse(data);
      
      if (!jsonData.hits || jsonData.hits.length === 0) {
        console.error('Нет доступных объявлений');
        return;
      }

      // Берем только 5 последних объявлений
      const properties = jsonData.hits.slice(0, 5);
      
      // Создаем CSV контент
      let csvContent = 'ID,Название,Цена,Комнаты,Ванные,Площадь,Регион,Тип аренды,Локация,Фото\n';
      
      properties.forEach(property => {
        const location = property.location ? property.location.map(loc => loc.name).join('; ') : '';
        const row = [
          property.externalID,
          `"${property.title}"`,
          property.price,
          property.rooms,
          property.baths,
          property.area,
          property.state,
          property.rentFrequency,
          `"${location}"`,
          property.coverPhoto?.url || ''
        ].join(',');
        
        csvContent += row + '\n';
      });

      // Сохраняем в файл
      fs.writeFileSync('bayut_properties.csv', csvContent);
      console.log('Данные успешно сохранены в bayut_properties.csv');

    } catch (error) {
      console.error('Ошибка при обработке данных:', error);
    }
  });
});

req.on('error', (error) => {
  console.error('Ошибка при запросе:', error);
});

req.end(); 
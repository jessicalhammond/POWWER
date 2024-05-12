import React from 'react';
// import Calendar from './Calendar';

export default {

  path: '/dashboard/calendar',

  async action() {
    const Calendar = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Calendar').default), 'calendar');
    });
    return <Calendar key={'calendar'} />;
  },
};

import React from 'react';
// import NProgress from 'nprogress';
// import Table from './Table';

export default {

  path: '/dashboard/table',

  async action() {
    const Table = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Table').default), 'table');
    });
    return <Table key={'table'} />;
  },

};

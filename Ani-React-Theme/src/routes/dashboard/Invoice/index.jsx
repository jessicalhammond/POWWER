import React from 'react';
// import Invoice from './Invoice';

export default {

  path: '/dashboard/invoice',

  async action() {
    const Invoice = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Invoice').default), 'invoice');
    });
    return <Invoice key={'invoice'} />;
  },
};

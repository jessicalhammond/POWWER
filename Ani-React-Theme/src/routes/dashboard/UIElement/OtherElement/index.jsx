import React from 'react';
// import NProgress from 'nprogress';
// import OtherElements from './OtherElements';

export default {

  path: '/dashboard/other-elements',

  async action() {
    const OtherElements = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./OtherElements').default), 'otherElements');
    });
    return <OtherElements key={'otherElements'} />;
  },

};

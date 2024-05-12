import React from 'react';
// import SignUp from './SignUp';

export default {

  path: '/signup',

  async action() {
    const SignUp = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./SignUp').default), 'signup');
    });
    return <SignUp key={'signup'} />;
  },
};
